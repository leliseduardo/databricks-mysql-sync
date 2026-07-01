"""
Envia um arquivo CSV para o Databricks via Unity Catalog Volume.

O CSV e enviado para um Volume e importado usando INSERT INTO ... SELECT
com cast explicito para respeitar o schema da tabela existente.

Uso:
    # Enviar CSV (cria/substitui a tabela com o nome do arquivo)
    python upload_csv_to_databricks.py /caminho/para/arquivo.csv

    # Definir nome da tabela manualmente
    python upload_csv_to_databricks.py /caminho/para/arquivo.csv --table minha_tabela

    # Modo append (adicionar sem apagar dados existentes)
    python upload_csv_to_databricks.py /caminho/para/arquivo.csv --mode append

    # Dry-run (mostra o que seria feito)
    python upload_csv_to_databricks.py /caminho/para/arquivo.csv --dry-run
"""

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from databricks import sql as databricks_sql
from dotenv import load_dotenv

load_dotenv()

VOLUME_NAME = "csv_uploads"


def get_databricks_connection(local_csv_dir: str):
    return databricks_sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
        staging_allowed_local_path=local_csv_dir,
    )


def ensure_volume_exists(cursor, catalog: str, schema: str):
    cursor.execute(
        f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{VOLUME_NAME}`"
    )


def get_table_schema(cursor, catalog: str, schema: str, table_name: str):
    """Retorna lista de (nome_coluna, tipo) da tabela existente."""
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


def build_insert_from_csv_sql(
    volume_path: str,
    catalog: str,
    schema: str,
    table_name: str,
    columns: list[tuple[str, str]],
) -> str:
    """Monta INSERT INTO ... SELECT com CAST explicito para cada coluna."""
    cast_exprs = []
    for col_name, col_type in columns:
        cast_exprs.append(f"TRY_CAST(`{col_name}` AS {col_type}) AS `{col_name}`")

    select_cols = ",\n        ".join(cast_exprs)
    return f"""
    INSERT INTO `{catalog}`.`{schema}`.`{table_name}`
    SELECT
        {select_cols}
    FROM read_files(
        '{volume_path}',
        format => 'csv',
        header => true,
        inferSchema => false
    )
    """


def upload_csv(
    csv_path: str,
    table_name: str,
    catalog: str,
    schema: str,
    mode: str,
    dry_run: bool,
):
    csv_path = os.path.abspath(csv_path)
    csv_dir = os.path.dirname(csv_path)
    csv_filename = os.path.basename(csv_path)
    volume_path = f"/Volumes/{catalog}/{schema}/{VOLUME_NAME}/{csv_filename}"
    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"

    file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"Arquivo: {csv_path}")
    print(f"Tamanho: {file_size_mb:.1f} MB")
    print(f"Destino tabela: {catalog}.{schema}.{table_name}")
    print(f"Destino volume: {volume_path}")
    print(f"Modo: {mode}")

    print("\nConectando ao Databricks...")
    conn = get_databricks_connection(csv_dir)
    cursor = conn.cursor()

    has_table = table_exists(cursor, catalog, schema, table_name)

    if mode == "append" and has_table:
        columns = get_table_schema(cursor, catalog, schema, table_name)
        print(f"\n  Schema da tabela existente ({len(columns)} colunas):")
        for col_name, col_type in columns:
            print(f"    {col_name}: {col_type}")
        strategy = "insert_select"
    elif mode == "replace" or not has_table:
        columns = None
        strategy = "copy_into"
    else:
        columns = None
        strategy = "copy_into"

    if dry_run:
        print(f"\n[DRY RUN] Passos que seriam executados:")
        print(f"  1. Criar volume '{VOLUME_NAME}' (se nao existir)")
        print(f"  2. Upload do CSV para {volume_path}")
        if mode == "replace":
            print(f"  3. DROP TABLE IF EXISTS {full_table}")
        if strategy == "insert_select":
            print(f"  3. INSERT INTO {full_table} SELECT (com CAST) FROM read_files()")
        else:
            print(f"  3. COPY INTO {full_table} FROM {volume_path}")
        print(f"  4. Remover CSV do volume")
        cursor.close()
        conn.close()
        return

    print(f"\nCriando volume '{VOLUME_NAME}' (se nao existir)...")
    ensure_volume_exists(cursor, catalog, schema)

    print(f"Enviando CSV para o volume...")
    start_upload = time.time()
    cursor.execute(f"PUT '{csv_path}' INTO '{volume_path}' OVERWRITE")
    upload_elapsed = time.time() - start_upload
    print(f"  Upload concluido em {upload_elapsed:.1f}s ({file_size_mb/upload_elapsed:.1f} MB/s)")

    if mode == "replace":
        print(f"Removendo tabela existente (mode=replace)...")
        cursor.execute(f"DROP TABLE IF EXISTS {full_table}")

    print(f"Importando dados...")
    start_import = time.time()

    if strategy == "insert_select":
        insert_sql = build_insert_from_csv_sql(
            volume_path, catalog, schema, table_name, columns
        )
        print(f"  Estrategia: INSERT INTO ... SELECT com CAST explicito")
        cursor.execute(insert_sql)
    else:
        print(f"  Estrategia: COPY INTO (tabela nova)")
        cursor.execute(f"""
            COPY INTO {full_table}
            FROM '{volume_path}'
            FILEFORMAT = CSV
            FORMAT_OPTIONS (
                'header' = 'true',
                'inferSchema' = 'true'
            )
        """)

    import_elapsed = time.time() - start_import
    print(f"  Importacao concluida em {import_elapsed:.1f}s")

    print(f"Verificando contagem de linhas...")
    cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
    row_count = cursor.fetchone()[0]
    print(f"  Linhas na tabela: {row_count:,}")

    print(f"Removendo CSV do volume...")
    cursor.execute(f"REMOVE '{volume_path}'")

    cursor.close()
    conn.close()

    total_elapsed = upload_elapsed + import_elapsed
    print(f"\nTempo total: {total_elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(
        description="Envia CSV para o Databricks via Volume"
    )
    parser.add_argument("csv_path", help="Caminho para o arquivo CSV")
    parser.add_argument(
        "--table",
        type=str,
        help="Nome da tabela no Databricks (default: nome do arquivo sem extensao)",
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

    csv_path = args.csv_path
    if not os.path.isfile(csv_path):
        print(f"ERRO: Arquivo nao encontrado: {csv_path}")
        sys.exit(1)

    catalog = os.getenv("DATABRICKS_CATALOG")
    schema = os.getenv("DATABRICKS_SCHEMA", "default")

    if not catalog:
        print("ERRO: DATABRICKS_CATALOG nao definido no .env")
        sys.exit(1)

    table_name = args.table or Path(csv_path).stem

    print(f"Upload CSV -> Databricks (via Volume)")
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.dry_run:
        print("*** MODO DRY-RUN ***")

    try:
        upload_csv(csv_path, table_name, catalog, schema, args.mode, args.dry_run)
    except Exception as e:
        print(f"\nERRO: {e}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"Upload finalizado!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
