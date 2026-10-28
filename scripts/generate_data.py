import argparse
import logging
import os
import sys

import psycopg2
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def obter_config():
    obrigatorias = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    faltando = [v for v in obrigatorias if not os.getenv(v)]
    if faltando:
        raise RuntimeError(f"Variáveis de ambiente ausentes: {', '.join(faltando)}")

    return {
        "host": os.environ["DB_HOST"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.environ["DB_NAME"],
        "sslmode": os.getenv("DB_SSLMODE", "prefer"),
    }


def conectar_banco():
    return psycopg2.connect(**obter_config())


def criar_tabela(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users_info (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(100),
                email VARCHAR(100) UNIQUE,
                idade INT,
                cidade VARCHAR(100)
            );
        """)
    conn.commit()


def gerar_dados(qtd=1000, dominios_variados=False):
    fake = Faker("pt_BR")
    return [
        (
            fake.name(),
            fake.email(safe=not dominios_variados),
            fake.random_int(min=18, max=80),
            fake.city(),
        )
        for _ in range(qtd)
    ]


def inserir_dados(conn, dados):
    query = """
        INSERT INTO users_info (nome, email, idade, cidade)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (email) DO NOTHING;
    """
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM users_info;")
        antes = cursor.fetchone()[0]
        cursor.executemany(query, dados)
        cursor.execute("SELECT COUNT(*) FROM users_info;")
        depois = cursor.fetchone()[0]
    conn.commit()

    inseridos = depois - antes
    log.info(
        "%d registros processados, %d inseridos, %d ignorados (e-mail duplicado). Total na tabela: %d",
        len(dados), inseridos, len(dados) - inseridos, depois,
    )
    return inseridos


def main(qtd=1000, dominios_variados=False):
    conn = conectar_banco()
    try:
        criar_tabela(conn)
        inserir_dados(conn, gerar_dados(qtd, dominios_variados))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gera dados fictícios e carrega na tabela users_info.")
    parser.add_argument("--qtd", type=int, default=1000, help="quantidade de registros a gerar")
    parser.add_argument(
        "--dominios-variados",
        action="store_true",
        help="gera e-mails com domínios realistas em vez de example.com/.net/.org",
    )
    args = parser.parse_args()

    try:
        main(args.qtd, args.dominios_variados)
    except Exception as e:
        log.error("Falha na carga: %s", e)
        sys.exit(1)
