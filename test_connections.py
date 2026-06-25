"""
Testa as conexoes com MySQL (via SSH tunnel) e Databricks separadamente.
Execute primeiro para garantir que as credenciais estao corretas.
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

load_dotenv()


def create_ssh_tunnel() -> SSHTunnelForwarder:
    """Cria tunel SSH para acessar o MySQL remoto."""
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


def test_mysql(tunnel: SSHTunnelForwarder):
    print("=" * 50)
    print("Testando conexao com MySQL (via SSH tunnel)...")
    print("=" * 50)
    print(f"  SSH tunnel: {os.getenv('SSH_HOST')} -> localhost:{tunnel.local_bind_port}")

    import mysql.connector

    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
        )
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        print(f"  MySQL conectado! Versao: {version[0]}")

        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"  Tabelas encontradas: {len(tables)}")
        for table in tables:
            print(f"    - {table[0]}")

        cursor.close()
        conn.close()
        print("  Conexao MySQL OK!\n")
        return True
    except Exception as e:
        print(f"  ERRO MySQL: {e}\n")
        return False


def test_databricks():
    print("=" * 50)
    print("Testando conexao com Databricks...")
    print("=" * 50)

    from databricks import sql

    try:
        conn = sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN"),
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS test")
        result = cursor.fetchone()
        print(f"  Databricks conectado! Query teste: {result[0]}")

        # Listar catalogos disponiveis
        cursor.execute("SHOW CATALOGS")
        catalogs = cursor.fetchall()
        print(f"\n  Catalogos disponiveis:")
        for cat in catalogs:
            print(f"    - {cat[0]}")

        catalog = os.getenv("DATABRICKS_CATALOG")
        if catalog:
            try:
                cursor.execute(f"USE CATALOG `{catalog}`")
                cursor.execute("SHOW SCHEMAS")
                schemas = cursor.fetchall()
                print(f"\n  Schemas no catalogo '{catalog}':")
                for s in schemas:
                    print(f"    - {s[0]}")
            except Exception as e:
                print(f"\n  AVISO: Catalogo '{catalog}' nao acessivel: {e}")
                print("  Defina DATABRICKS_CATALOG no .env com um dos catalogos acima.")
        else:
            print("\n  DATABRICKS_CATALOG esta vazio no .env.")
            print("  Defina com um dos catalogos acima para continuar.")

        cursor.close()
        conn.close()
        print("\n  Conexao Databricks OK!\n")
        return True
    except Exception as e:
        print(f"  ERRO Databricks: {e}\n")
        return False


if __name__ == "__main__":
    # Testar SSH + MySQL
    tunnel = None
    mysql_ok = False
    try:
        tunnel = create_ssh_tunnel()
        print(f"  SSH tunnel aberto na porta local {tunnel.local_bind_port}\n")
        mysql_ok = test_mysql(tunnel)
    except Exception as e:
        print(f"  ERRO ao criar SSH tunnel: {e}\n")
    finally:
        if tunnel:
            tunnel.stop()

    # Testar Databricks
    databricks_ok = test_databricks()

    print("=" * 50)
    print("RESULTADO")
    print("=" * 50)
    print(f"  MySQL (SSH):  {'OK' if mysql_ok else 'FALHOU'}")
    print(f"  Databricks:   {'OK' if databricks_ok else 'FALHOU'}")

    if mysql_ok and databricks_ok:
        print("\n  Tudo pronto! Execute sync_mysql_to_databricks.py para sincronizar.")
    else:
        print("\n  Corrija os erros acima antes de sincronizar.")
        sys.exit(1)
