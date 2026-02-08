import subprocess
import sys
import time
import signal
from datetime import datetime, timedelta

from src.utils import registrar_execucao


experimentos = {

    "pandas_full_load_10gb.py": {
        "codigo": "pandas_full_load_10gb",
        "tamanho_do_dado": "10GB",
    },

    "pandas_full_load_50gb.py": {
        "codigo": "pandas_full_load",
        "tamanho_do_dado": "50GB",
    },

    "pandas_full_load_100gb.py": {
        "codigo": "pandas_full_load",
        "tamanho_do_dado": "100GB",
    },

    "pandas_incremental_10gb.py": {
        "codigo": "pandas_incremental",
        "tamanho_do_dado": "10_gb",
    },


    "pandas_incremental_50gb.py": {
        "codigo": "pandas_incremental",
        "tamanho_do_dado": "50_gb",
    },


    "pandas_incremental_100gb.py": {
        "codigo": "pandas_incremental",
        "tamanho_do_dado": "100_gb",
    },

    "duckdb_incremental_10gb.py": {
        "codigo": "duckdb_incremental",
        "tamanho_do_dado": "10GB",
    },
    "spark_incremental_10gb.py": {
        "codigo": "spark_incremental",
        "tamanho_do_dado": "10GB",
    },

    "duckdb_incremental_50gb.py": {
        "codigo": "duckdb_incremental",
        "tamanho_do_dado": "50GB",
    },
    "spark_incremental_50gb.py": {
        "codigo": "spark_incremental",
        "tamanho_do_dado": "50GB",
    },

    "duckdb_incremental_100gb.py": {
        "codigo": "duckdb_incremental",
        "tamanho_do_dado": "100GB",
    },
    
    "spark_incremental_100gb.py": {
        "codigo": "spark_incremental",
        "tamanho_do_dado": "100GB",
    },

    "duckdb_full_load_10gb.py": {
        "codigo": "duckdb_full_load",
        "tamanho_do_dado": "10GB",
    },
    "spark_full_load_10gb.py": {
        "codigo": "spark_full_load",
        "tamanho_do_dado": "10GB",
    },

    "duckdb_full_load_50gb.py": {
        "codigo": "duckdb_full_load",
        "tamanho_do_dado": "50GB",
    },
    "spark_full_load_50gb.py": {
        "codigo": "spark_full_load",
        "tamanho_do_dado": "50GB",
    },

    "duckdb_full_load_100gb.py": {
        "codigo": "duckdb_full_load",
        "tamanho_do_dado": "100GB",
    },
    
    "spark_full_load_100gb.py": {
        "codigo": "spark_full_load",
        "tamanho_do_dado": "100GB",
    },
}

for script, config in experimentos.items():
    codigo = config["codigo"]
    tamanho_do_dado = config["tamanho_do_dado"]

    data_atual = (datetime.now() - timedelta(hours=3)).strftime(
        "%Y-%m-%d %Hh:%Mmin"
    )

    print(f"\n▶ Rodando: {script}")

    start = time.time()
    end = None
    status = None

    try:
        result = subprocess.run(
            [sys.executable, script],
            capture_output=True,
            text=True
        )

        print(result.stdout)

        if result.returncode == 0:
            end = time.time()
            status = "SUCCESS"

        elif result.returncode in (-signal.SIGKILL, 137):
            print("💀 Processo morto por OOM / SIGKILL")
            status = "KILLED"

        else:
            print(f"❌ Erro ao rodar {script}")
            print(result.stderr)
            status = "ERROR"

    except Exception as e:
        print(f"💥 Falha inesperada: {e}")
        status = "ERROR"

    finally:
        registrar_execucao(
            data_atual=data_atual,
            codigo=codigo,
            tamanho_do_dado=tamanho_do_dado,
            start=start,
            end=end,
            status=status,
            type="total",
            description="tempo total de execução"
        )
