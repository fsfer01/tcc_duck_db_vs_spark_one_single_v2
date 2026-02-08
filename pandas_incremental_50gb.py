import time
import os
import glob
import sys
import signal
from datetime import datetime, timedelta
import multiprocessing as mp
import pandas as pd

from src.utils import SystemMonitor, registrar_execucao

# ===================================================
# CONFIGURAÇÃO GLOBAL DO MULTIPROCESSING
# ===================================================
mp.set_start_method("spawn", force=True)  # evita deadlocks e problemas de fork

# ===================================================
# CONSULTAS PYTHON (substituem os SQLs)
# ===================================================
def consulta_1(df: pd.DataFrame):
    """Consulta simples: count total e count 1"""
    return pd.DataFrame({
        "count_total": [len(df)],
        "count_line": [len(df)]
    })

def consulta_2(df: pd.DataFrame):
    """Agregações simples: COUNT e SUM"""
    return pd.DataFrame({
        "sample_id": [df['SAMPLE_ID'].count()],
        "similarity": [df['similarity'].sum()]
    })

def consulta_3(df: pd.DataFrame):
    """Agregações pesadas: COUNT DISTINCT de várias colunas"""
    return pd.DataFrame({
        "sample_id": [df['SAMPLE_ID'].nunique()],
        "url": [df['URL'].nunique()],
        "text": [df['TEXT'].nunique()],
        "height": [df['HEIGHT'].nunique()],
        "width": [df['WIDTH'].nunique()],
        "license": [df['LICENSE'].nunique()],
        "nsfw": [df['NSFW'].nunique()],
        "similarity": [df['similarity'].nunique()]
    })

# Mapeamento consultas
CONSULTAS = {
    "consulta_1_count_lines": consulta_1,
    "consulta_2_agreg_sample": consulta_2,
    "consulta_3_agreg_columns": consulta_3
}

# ---------------------------------------------------
# Caminhos
# ---------------------------------------------------
# ---------------------------------------------------
# VARIÁVEIS GLOBAIS
# ---------------------------------------------------
data_atual = (datetime.now() - timedelta(hours=3)).strftime("%Y-%m-%d %Hh:%Mmin")
codigo = 'pandas_incremental'
tamanho_do_dado = '50_gb'
start_total = time.time()

parquet_path = f"/media/sf_HD_EXTERNO/bucket/{tamanho_do_dado}/parquets/*.parquet"
parquet_files = glob.glob(parquet_path)

# Pasta com nomes das queries (agora não precisamos dos arquivos .sql)
query_names = list(CONSULTAS.keys())

if not parquet_files:
    raise RuntimeError("Nenhum parquet encontrado")

timestamp_str = (datetime.now() - timedelta(hours=3)).strftime("%Y_%m_%d")

# ---------------------------------------------------
# Execução com Pandas
# ---------------------------------------------------
qtd_de_repeticoes = 1
for exec_id in range(1, qtd_de_repeticoes+1):
    print(f"\n🚀 Iniciando execução {exec_id}/{qtd_de_repeticoes}")

    # ---------------------------------------------------
    # Loop nas queries (agora usando as funções CONSULTAS)
    # ---------------------------------------------------
    for query_name in query_names:
        codigo_query = f"{codigo}_{query_name}_{tamanho_do_dado}_{exec_id}_de_{qtd_de_repeticoes}"
        log_filename = f"logs/{codigo_query}.csv"

        monitor = SystemMonitor(
            output_file=log_filename,
            script_name=codigo_query,
            engine="python"
        )
        monitor.start_monitoring()

        print(f"\n▶ Executando query: {query_name}")

        # Obter a função de consulta correspondente
        consulta_func = CONSULTAS[query_name]
        
        start_query = time.time()
        status = "success"

        try:
            # Loop nos parquets
            for file_path in parquet_files:
                file_name = os.path.basename(file_path)

                # Ler o arquivo parquet com pandas
                print(f"    📂 Lendo arquivo: {file_name}")
                df = pd.read_parquet(file_path)
                
                # Aplicar a função de consulta
                resultado = consulta_func(df)
                
                # Salvar resultado em Excel
                xlsx_path = f"resultados_xlsx/resultado_{codigo_query}.xlsx"
                resultado.to_excel(xlsx_path, index=False)
                print(f"    ✅ Resultado salvo em: {xlsx_path}")

        except Exception as e:
            print(f"    ❌ Erro: {e}")
            print(f"    Parquet que deu erro: {file_path}")
            status = "error"

        finally:
            # Log do tempo total da query
            registrar_execucao(
                data_atual=data_atual,
                codigo=codigo_query,
                tamanho_do_dado=tamanho_do_dado,
                start=start_query,
                end=time.time(),
                status=status,
                type="pandas",
                description=query_name
            )
            monitor.stop_monitoring()

# Tempo total de execução
end_total = time.time()
tempo_total = end_total - start_total
minutos = int(tempo_total // 60)
segundos = int(tempo_total % 60)

print(f"\n⏱ Tempo total de execução do script: {minutos} min {segundos} s")
print(f"{codigo}_{tamanho_do_dado} - Todas as consultas finalizadas.")