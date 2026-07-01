"""
Pipeline completo: dump MySQL -> CSV -> upload Databricks.

Extrai dados da tabela clients_integrations do MySQL (via SSH tunnel),
salva em CSV temporario e envia para o Databricks via Volume.

Uso:
    # Primeira sincronizacao (todos os dados)
    python pipeline.py --full

    # Sincronizacao incremental (ultimos 7 dias por updated_at)
    python pipeline.py

    # Incremental com janela customizada (ex: 14 dias)
    python pipeline.py --days 14

    # Dry-run (mostra o que seria feito)
    python pipeline.py --dry-run

    # Tabela especifica (default: clients_integrations)
    python pipeline.py --table outra_tabela
"""

import argparse
import csv
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta

import mysql.connector
from databricks import sql as databricks_sql
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

load_dotenv()

VOLUME_NAME = "csv_uploads"
DEFAULT_TABLE = "clients_integrations"


# === Conexoes ===

def create_ssh_tunnel() -> SSHTunnelForwarder:
    ssh_key = os.path.expanduser(os.getenv("SSH_KEY_PATH", "~/.ssh/id_rsa"))
    tunnel = SSHTunnelForwarder(
        (os.getenv("SSH_HOST"), int(os.getenv("SSH_PORT", "22"))),
        ssh_username=os.getenv("SSH_USER", "root"),
        ssh_pkey=ssh_key,
        remote_bind_address=(
            os.getenv("MYSQL_HOST", "127.0.0.1"),
            int(os.getenv("MYSQL_PORT", "3306")),
        ),
    )
    tunnel.start()
    return tunnel


def get_mysql_connection(tunnel: SSHTunnelForwarder):
    return mysql.connector.connect(
        host="127.0.0.1",
        port=tunnel.local_bind_port,
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )


def get_databricks_connection(staging_path: str):
    return databricks_sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
        staging_allowed_local_path=staging_path,
    )


# === Etapa 1: Dump MySQL -> CSV ===

def dump_mysql_to_csv(
    tunnel: SSHTunnelForwarder,
    table_name: str,
    csv_path: str,
    full: bool,
    days: int,
) -> int:
    conn = get_mysql_connection(tunnel)
    cursor = conn.cursor()

    if full:
        query = f"SELECT * FROM `{table_name}`"
        print(f"  Query: SELECT * FROM {table_name} (full)")
    else:
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        query = f"SELECT * FROM `{table_name}` WHERE `updated_at` >= '{since}'"
        print(f"  Query: ... WHERE updated_at >= '{since}' (ultimos {days} dias)")

    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]

    row_count = 0
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(columns)

        while True:
            rows = cursor.fetchmany(5000)
            if not rows:
                break
            for row in rows:
                clean = []
                for val in row:
                    if val is None:
                        clean.append("")
                    elif isinstance(val, (bytearray, bytes)):
                        clean.append(val.hex())
                    elif isinstance(val, set):
                        clean.append(",".join(sorted(val)))
                    else:
                        clean.append(str(val))
                writer.writerow(clean)
            row_count += len(rows)
            print(f"  Extraidas: {row_count:,} linhas", end="\r")

    print(f"  Extraidas: {row_count:,} linhas")
    cursor.close()
    conn.close()
    return row_count


# === Etapa 2: Upload CSV -> Databricks ===

def get_table_schema(cursor, catalog: str, schema: str, table_name: str):
    cursor.execute(f"DESCRIBE TABLE `{catalog}`.`{schema}`.`{table_name}`")
    columns = []
    for row in cursor.fetchall():
        col_name, col_type = row[0], row[1]
        if col_name.startswith("#") or col_name == "":
            break
        columns.append((col_name, col_type))
    return columns


def table_exists(cursor, catalog: str, schema: str, table_name: str) -> bool:
    try:
        cursor.execute(
            f"SELECT 1 FROM `{catalog}`.`{schema}`.`{table_name}` LIMIT 1"
        )
        cursor.fetchall()
        return True
    except Exception:
        return False


def upload_to_databricks(
    csv_path: str,
    table_name: str,
    catalog: str,
    schema: str,
    full: bool,
) -> int:
    csv_dir = os.path.dirname(csv_path)
    csv_filename = os.path.basename(csv_path)
    volume_path = f"/Volumes/{catalog}/{schema}/{VOLUME_NAME}/{csv_filename}"
    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"

    conn = get_databricks_connection(csv_dir)
    cursor = conn.cursor()

    cursor.execute(
        f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{VOLUME_NAME}`"
    )

    file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"  Enviando CSV ({file_size_mb:.1f} MB) para o volume...")
    start = time.time()
    cursor.execute(f"PUT '{csv_path}' INTO '{volume_path}' OVERWRITE")
    print(f"  Upload concluido em {time.time() - start:.1f}s")

    has_table = table_exists(cursor, catalog, schema, table_name)

    if full and has_table:
        print(f"  Modo full: removendo tabela existente...")
        cursor.execute(f"DROP TABLE IF EXISTS {full_table}")
        has_table = False

    print(f"  Importando dados...")
    start = time.time()

    if has_table:
        columns = get_table_schema(cursor, catalog, schema, table_name)
        cast_exprs = [
            f"TRY_CAST(`{c}` AS {t}) AS `{c}`" for c, t in columns
        ]
        select_cols = ", ".join(cast_exprs)
        update_set = ", ".join(
            f"target.`{c}` = src_cast.`{c}`" for c, _ in columns if c != "id"
        )
        insert_cols = ", ".join(f"`{c}`" for c, _ in columns)
        insert_vals = ", ".join(f"src_cast.`{c}`" for c, _ in columns)
        cursor.execute(f"""
            MERGE INTO {full_table} AS target
            USING (
                SELECT {select_cols}
                FROM read_files(
                    '{volume_path}',
                    format => 'csv',
                    header => true,
                    inferSchema => false
                )
            ) AS src_cast
            ON target.`id` = src_cast.`id`
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        print(f"  Estrategia: MERGE (upsert por id)")
    else:
        cursor.execute(f"""
            CREATE TABLE {full_table} AS
            SELECT *
            FROM read_files(
                '{volume_path}',
                format => 'csv',
                header => true,
                inferSchema => true
            )
        """)

    print(f"  Importacao concluida em {time.time() - start:.1f}s")

    cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
    total_rows = cursor.fetchone()[0]

    print(f"  Removendo CSV do volume...")
    cursor.execute(f"REMOVE '{volume_path}'")

    cursor.close()
    conn.close()
    return total_rows


# === Pipeline ===

def run_pipeline(table_name: str, full: bool, days: int, dry_run: bool):
    catalog = os.getenv("DATABRICKS_CATALOG")
    schema = os.getenv("DATABRICKS_SCHEMA", "default")

    if not catalog:
        print("ERRO: DATABRICKS_CATALOG nao definido no .env")
        sys.exit(1)

    mode_label = "FULL" if full else f"INCREMENTAL (ultimos {days} dias)"
    print(f"Pipeline: MySQL -> CSV -> Databricks")
    print(f"Tabela: {table_name}")
    print(f"Modo: {mode_label}")
    print(f"Destino: {catalog}.{schema}.{table_name}")
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if dry_run:
        print("*** MODO DRY-RUN ***")

    # Etapa 1: Dump MySQL -> CSV
    print(f"\n{'='*60}")
    print(f"  ETAPA 1: Dump MySQL -> CSV")
    print(f"{'='*60}")

    if dry_run:
        if full:
            print(f"  [DRY RUN] SELECT * FROM {table_name}")
        else:
            since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            print(f"  [DRY RUN] SELECT * FROM {table_name} WHERE updated_at >= '{since}'")
        print(f"  [DRY RUN] Salvaria em CSV temporario")
        print(f"\n{'='*60}")
        print(f"  ETAPA 2: Upload CSV -> Databricks")
        print(f"{'='*60}")
        if full:
            print(f"  [DRY RUN] DROP TABLE + COPY INTO (tabela nova)")
        else:
            print(f"  [DRY RUN] INSERT INTO ... SELECT com TRY_CAST (append)")
        return

    print(f"  Abrindo SSH tunnel...")
    tunnel = create_ssh_tunnel()
    print(f"  SSH tunnel aberto na porta local {tunnel.local_bind_port}")

    csv_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=f"_{table_name}.csv", delete=False, dir="/tmp"
        ) as tmp:
            csv_path = tmp.name

        start = time.time()
        row_count = dump_mysql_to_csv(tunnel, table_name, csv_path, full, days)
        dump_elapsed = time.time() - start

        file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
        print(f"  CSV: {csv_path}")
        print(f"  Tamanho: {file_size_mb:.1f} MB | Linhas: {row_count:,}")
        print(f"  Tempo: {dump_elapsed:.1f}s")

        tunnel.stop()
        print(f"  SSH tunnel fechado.")

        if row_count == 0:
            print(f"\n  Nenhuma linha encontrada. Nada a enviar.")
            return

        # Etapa 2: Upload CSV -> Databricks
        print(f"\n{'='*60}")
        print(f"  ETAPA 2: Upload CSV -> Databricks")
        print(f"{'='*60}")

        start = time.time()
        total_rows = upload_to_databricks(csv_path, table_name, catalog, schema, full)
        upload_elapsed = time.time() - start

        print(f"\n{'='*60}")
        print(f"  RESULTADO")
        print(f"{'='*60}")
        print(f"  Linhas extraidas do MySQL: {row_count:,}")
        print(f"  Total de linhas na tabela: {total_rows:,}")
        print(f"  Tempo dump:   {dump_elapsed:.1f}s")
        print(f"  Tempo upload: {upload_elapsed:.1f}s")
        print(f"  Tempo total:  {dump_elapsed + upload_elapsed:.1f}s")
        print(f"  Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    finally:
        if csv_path and os.path.exists(csv_path):
            os.unlink(csv_path)
            print(f"  CSV temporario removido.")
        if tunnel.is_active:
            tunnel.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline: dump MySQL -> CSV -> Databricks"
    )
    parser.add_argument(
        "--table",
        type=str,
        default=DEFAULT_TABLE,
        help=f"Tabela a sincronizar (default: {DEFAULT_TABLE})",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Sincronizacao completa (todos os dados). Sem essa flag, faz incremental.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Janela de dias para sync incremental (default: 7)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra o que seria feito sem executar",
    )
    args = parser.parse_args()

    try:
        run_pipeline(args.table, args.full, args.days, args.dry_run)
    except Exception as e:
        print(f"\nERRO: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
