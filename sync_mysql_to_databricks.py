"""
Sincroniza tabelas do MySQL (via SSH tunnel) para o Databricks.

Uso:
    # Sincronizar todas as tabelas do MySQL
    python sync_mysql_to_databricks.py

    # Sincronizar tabelas especificas
    python sync_mysql_to_databricks.py --tables usuarios,pedidos,produtos

    # Modo dry-run (mostra o que seria feito sem executar)
    python sync_mysql_to_databricks.py --dry-run

    # Limpar tabelas destino antes de inserir (full refresh)
    python sync_mysql_to_databricks.py --mode replace

    # Append (inserir sem limpar)
    python sync_mysql_to_databricks.py --mode append
"""

import argparse
import os
import sys
import time
from datetime import datetime

import mysql.connector
from databricks import sql as databricks_sql
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

load_dotenv()

BATCH_SIZE = 1000

MYSQL_TO_DATABRICKS_TYPES = {
    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "mediumint": "INT",
    "int": "INT",
    "integer": "INT",
    "bigint": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "decimal": "DECIMAL",
    "numeric": "DECIMAL",
    "char": "STRING",
    "varchar": "STRING",
    "tinytext": "STRING",
    "text": "STRING",
    "mediumtext": "STRING",
    "longtext": "STRING",
    "enum": "STRING",
    "set": "STRING",
    "json": "STRING",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "timestamp": "TIMESTAMP",
    "time": "STRING",
    "year": "INT",
    "tinyblob": "BINARY",
    "blob": "BINARY",
    "mediumblob": "BINARY",
    "longblob": "BINARY",
    "binary": "BINARY",
    "varbinary": "BINARY",
    "bit": "BOOLEAN",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
}


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


def get_databricks_connection():
    return databricks_sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    )


def get_mysql_tables(mysql_conn) -> list[str]:
    cursor = mysql_conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables


def get_mysql_columns(mysql_conn, table_name: str) -> list[dict]:
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute(f"DESCRIBE `{table_name}`")
    columns = cursor.fetchall()
    cursor.close()
    return columns


def mysql_type_to_databricks(mysql_type: str) -> str:
    base_type = mysql_type.split("(")[0].split(" ")[0].lower()
    return MYSQL_TO_DATABRICKS_TYPES.get(base_type, "STRING")


def build_create_table_sql(
    table_name: str, columns: list[dict], catalog: str, schema: str
) -> str:
    col_defs = []
    for col in columns:
        db_type = mysql_type_to_databricks(col["Type"])
        col_name = col["Field"]
        col_defs.append(f"`{col_name}` {db_type}")

    cols_str = ",\n    ".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{table_name}` (\n    {cols_str}\n)"


def get_row_count(mysql_conn, table_name: str) -> int:
    cursor = mysql_conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    count = cursor.fetchone()[0]
    cursor.close()
    return count


def sync_table(
    mysql_conn,
    databricks_conn,
    table_name: str,
    catalog: str,
    schema: str,
    mode: str,
    dry_run: bool,
):
    print(f"\n{'='*60}")
    print(f"  Tabela: {table_name}")
    print(f"{'='*60}")

    columns = get_mysql_columns(mysql_conn, table_name)
    col_names = [col["Field"] for col in columns]
    row_count = get_row_count(mysql_conn, table_name)
    print(f"  Colunas: {len(col_names)}")
    print(f"  Linhas no MySQL: {row_count:,}")

    if dry_run:
        create_sql = build_create_table_sql(table_name, columns, catalog, schema)
        print(f"  [DRY RUN] SQL de criacao:\n    {create_sql}")
        print(f"  [DRY RUN] Seriam inseridas {row_count:,} linhas em lotes de {BATCH_SIZE}")
        return

    db_cursor = databricks_conn.cursor()

    create_sql = build_create_table_sql(table_name, columns, catalog, schema)
    if mode == "replace":
        print("  Removendo tabela existente (mode=replace)...")
        db_cursor.execute(
            f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{table_name}`"
        )
    db_cursor.execute(create_sql)
    print("  Tabela criada/verificada no Databricks.")

    if row_count == 0:
        print("  Tabela vazia no MySQL, nada a sincronizar.")
        db_cursor.close()
        return

    mysql_cursor = mysql_conn.cursor()
    escaped_cols = ", ".join(f"`{c}`" for c in col_names)
    mysql_cursor.execute(f"SELECT {escaped_cols} FROM `{table_name}`")

    placeholders = ", ".join(["?"] * len(col_names))
    insert_cols = ", ".join(f"`{c}`" for c in col_names)
    insert_sql = (
        f"INSERT INTO `{catalog}`.`{schema}`.`{table_name}` "
        f"({insert_cols}) VALUES ({placeholders})"
    )

    total_inserted = 0
    start_time = time.time()

    while True:
        rows = mysql_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        clean_rows = []
        for row in rows:
            clean_row = []
            for val in row:
                if isinstance(val, bytearray):
                    clean_row.append(bytes(val))
                elif isinstance(val, set):
                    clean_row.append(",".join(sorted(val)))
                else:
                    clean_row.append(val)
            clean_rows.append(clean_row)

        db_cursor.executemany(insert_sql, clean_rows)
        total_inserted += len(clean_rows)

        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        pct = (total_inserted / row_count) * 100
        print(
            f"  Progresso: {total_inserted:,}/{row_count:,} ({pct:.1f}%) "
            f"- {rate:.0f} linhas/s",
            end="\r",
        )

    elapsed = time.time() - start_time
    print(
        f"\n  Concluido: {total_inserted:,} linhas em {elapsed:.1f}s "
        f"({total_inserted/elapsed:.0f} linhas/s)"
    )

    mysql_cursor.close()
    db_cursor.close()


def main():
    parser = argparse.ArgumentParser(
        description="Sincroniza tabelas MySQL -> Databricks"
    )
    parser.add_argument(
        "--tables",
        type=str,
        help="Tabelas a sincronizar (separadas por virgula). Default: todas.",
    )
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="replace = limpa e recria; append = insere sem limpar (default: replace)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra o que seria feito sem executar",
    )
    args = parser.parse_args()

    catalog = os.getenv("DATABRICKS_CATALOG")
    schema = os.getenv("DATABRICKS_SCHEMA", "default")
    db_name = os.getenv("MYSQL_DATABASE")

    if not catalog:
        print("ERRO: DATABRICKS_CATALOG nao definido no .env")
        print("Execute test_connections.py primeiro para ver os catalogos disponiveis.")
        sys.exit(1)

    print(f"Sincronizacao MySQL -> Databricks")
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"MySQL database: {db_name}")
    print(f"Databricks destino: {catalog}.{schema}")
    print(f"Modo: {args.mode}")
    if args.dry_run:
        print("*** MODO DRY-RUN ***")

    # Abrir SSH tunnel
    print("Abrindo SSH tunnel...")
    tunnel = create_ssh_tunnel()
    print(f"SSH tunnel aberto na porta local {tunnel.local_bind_port}")

    try:
        mysql_conn = get_mysql_connection(tunnel)
        print("Conectado ao MySQL.")

        if args.dry_run:
            databricks_conn = None
            print("[DRY RUN] Conexao com Databricks nao sera estabelecida.")
        else:
            databricks_conn = get_databricks_connection()
            print("Conectado ao Databricks.")

        if args.tables:
            tables = [t.strip() for t in args.tables.split(",")]
        else:
            tables = get_mysql_tables(mysql_conn)

        print(f"Tabelas a sincronizar: {len(tables)}")
        total_start = time.time()

        for table in tables:
            try:
                sync_table(
                    mysql_conn, databricks_conn, table, catalog, schema, args.mode, args.dry_run
                )
            except Exception as e:
                print(f"\n  ERRO ao sincronizar '{table}': {e}")
                print("  Continuando com a proxima tabela...")

        total_elapsed = time.time() - total_start
        print(f"\n{'='*60}")
        print(f"Sincronizacao finalizada em {total_elapsed:.1f}s")
        print(f"{'='*60}")

        mysql_conn.close()
        if databricks_conn:
            databricks_conn.close()
    finally:
        tunnel.stop()
        print("SSH tunnel fechado.")


if __name__ == "__main__":
    main()
