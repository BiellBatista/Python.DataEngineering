"""Interface de linha de comando SQL interativa para arquivos Parquet."""

from pathlib import Path

import duckdb

from config import BRONZE_COTAHIST_DIR, SILVER_COTAHIST_DIR

# Permite autocompletar e navegar no histórico com as setas do teclado (Linux/WSL).
try:
    import readline
except ImportError:
    pass


def start_interactive_cli(
    bronze_dir: str = BRONZE_COTAHIST_DIR,
    silver_dir: str = SILVER_COTAHIST_DIR,
) -> None:
    """Abre um REPL DuckDB com views para as camadas Bronze e Silver.

    Args:
        bronze_dir: Diretório raiz dos Parquets Bronze.
        silver_dir: Diretório raiz dos Parquets Silver.

    Note:
        Os atalhos ``schema`` e ``count`` consultam a view Silver, também
        disponível pelo nome legado ``cotacoes``. As consultas SQL podem usar
        diretamente as views ``bronze`` e ``silver``.
    """
    print("=" * 70)
    print("DuckDB Interactive SQL CLI - Camadas Bronze e Silver B3")
    print("=" * 70)
    print(f"Bronze: {bronze_dir}")
    print(f"Silver: {silver_dir}")
    print("Views: bronze, silver e cotacoes (alias de silver)")
    print("Comandos especiais: schema, count, exit, quit, sair")
    print("Exemplos:")
    print("  SELECT * FROM bronze LIMIT 5;")
    print("  SELECT ticker, data_pregao, preco_fechamento FROM silver LIMIT 5;\n")

    con = duckdb.connect(":memory:")
    try:
        try:
            _register_parquet_views(con, bronze_dir, silver_dir)
        except Exception as exc:
            print(f"Erro ao registrar os arquivos Parquet: {exc}")
            return

        while True:
            try:
                user_input = input("SQL> ").strip()
                if not user_input:
                    continue

                if user_input.lower() in ("exit", "quit", "sair"):
                    print("Até logo!")
                    break

                if user_input.lower() == "schema":
                    user_input = "DESCRIBE silver;"
                elif user_input.lower() == "count":
                    user_input = "SELECT count(*) AS total_registros FROM silver;"

                con.sql(user_input).show(max_rows=50)
            except KeyboardInterrupt:
                print("\nOperação cancelada. Digite 'exit' para sair.")
            except EOFError:
                print("\nAté logo!")
                break
            except Exception as err:
                print(f"Erro na execução do SQL: {err}\n")
    finally:
        con.close()


def _register_parquet_views(
    con: duckdb.DuckDBPyConnection,
    bronze_dir: str,
    silver_dir: str,
) -> None:
    """Registra views DuckDB sobre todos os Parquets das duas camadas.

    Args:
        con: Conexão DuckDB na qual criar as views.
        bronze_dir: Diretório raiz dos arquivos Parquet Bronze.
        silver_dir: Diretório raiz dos arquivos Parquet Silver.

    Raises:
        FileNotFoundError: Se uma das camadas não contiver arquivos Parquet.
        duckdb.Error: Se DuckDB não conseguir criar uma view sobre os arquivos.
    """
    for view_name, directory in (
        ("bronze", bronze_dir),
        ("silver", silver_dir),
    ):
        layer_path = Path(directory)
        if not any(layer_path.rglob("*.parquet")):
            raise FileNotFoundError(
                f"Nenhum arquivo Parquet encontrado para {view_name}: {directory}"
            )

        parquet_pattern = str(layer_path / "**" / "*.parquet")
        escaped_pattern = parquet_pattern.replace("'", "''")
        con.execute(
            f"""
            CREATE VIEW {view_name} AS
            SELECT *
            FROM read_parquet('{escaped_pattern}', hive_partitioning = true)
            """
        )

    con.execute("CREATE VIEW cotacoes AS SELECT * FROM silver;")


if __name__ == "__main__":
    start_interactive_cli()
