import os
import sys
import duckdb

# Permite autocompletar e navegar no histórico com as setas do teclado (Linux/WSL)
try:
    import readline
except ImportError:
    pass

SILVER_DIR = "/mnt/d/b3_datalake/silver/cotacoes_historicas"

def start_interactive_cli(silver_dir: str):
    parquet_pattern = f"{silver_dir.rstrip('/')}/**/*.parquet"
    
    print("=" * 70)
    print(" 🚀 DuckDB Interactive SQL CLI - Camada Silver B3")
    print("=" * 70)
    print(f"📁 Lendo dados de: {parquet_pattern}")
    print("💡 View criada: 'cotacoes' (ou 'silver')")
    print("📌 Comandos especiais:")
    print("   - 'exit' ou 'quit' : Sair da aplicação")
    print("   - 'schema'        : Exibir estrutura da tabela")
    print("   - 'count'         : Exibir total de registros")
    print("=" * 70)
    print("Exemplo de consulta:")
    print("   SELECT ticker, data_pregao, preco_fechamento FROM cotacoes LIMIT 5;\n")

    # Conexão em memória
    con = duckdb.connect(":memory:")

    # Criação da VIEW unificada com hive_partitioning habilitado
    try:
        create_view_sql = f"""
            CREATE VIEW cotacoes AS 
            SELECT * 
            FROM read_parquet('{parquet_pattern}', hive_partitioning = true);
        """
        con.execute(create_view_sql)
        con.execute("CREATE VIEW silver AS SELECT * FROM cotacoes;")
    except Exception as e:
        print(f"❌ Erro ao registrar os arquivos Parquet: {e}")
        return

    # Loop Interativo (REPL)
    while True:
        try:
            # Captura a query do usuário no terminal
            user_input = input("SQL> ").strip()

            # Trata comando de saída
            if not user_input:
                continue

            if user_input.lower() in ("exit", "quit", "sair"):
                print("Até logo! 👋")
                break

            # Atatalhos úteis
            if user_input.lower() == "schema":
                user_input = "DESCRIBE cotacoes;"
            elif user_input.lower() == "count":
                user_input = "SELECT count(*) AS total_registros FROM cotacoes;"

            # Executa a query no DuckDB e exibe formatado no terminal
            # O método .show() do DuckDB imprime em formato de tabela elegante
            con.sql(user_input).show(max_rows=50)

        except KeyboardInterrupt:
            print("\nOperação cancelada pelo usuário. Digite 'exit' para sair.")
        except EOFError:
            print("\nAté logo! 👋")
            break
        except Exception as err:
            print(f"❌ Erro na execução do SQL: {err}\n")

if __name__ == "__main__":
    start_interactive_cli(SILVER_DIR)