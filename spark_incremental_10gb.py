import pandas
import time
import os
import glob
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from src.utils import SystemMonitor, registrar_execucao

# ---------------------------------------------------
# VARIÁVEIS GLOBAIS
# ---------------------------------------------------
data_atual = (datetime.now() - timedelta(hours=3)).strftime("%Y-%m-%d %Hh:%Mmin")
codigo = "spark_incremental"
tamanho_do_dado = "10GB"
# no topo do seu script
start_total = time.time()

# ---------------------------------------------------
# CONFIG Spark
# ---------------------------------------------------
spark = (
    SparkSession.builder
        .appName("spark-metrics-test")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "16g")
        .config("spark.executor.memory", "16g")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------
# Caminhos
# ---------------------------------------------------
parquet_path = "/media/sf_HD_EXTERNO/bucket/10_gb/parquets/*.parquet"
parquet_files = glob.glob(parquet_path)

sql_folder = "/media/sf_HD_EXTERNO/bucket/10_gb/consultas_sql/*.sql"
sql_files = sorted(glob.glob(sql_folder))

if not parquet_files:
    raise RuntimeError("Nenhum parquet encontrado")

if not sql_files:
    raise RuntimeError("Nenhum SQL encontrado")

timestamp_str = (datetime.now() - timedelta(hours=3)).strftime("%Y_%m_%d")

qtd_de_repeticoes = 16
for exec_id in range(1, qtd_de_repeticoes):
    print(f"\n🚀 Iniciando execução {exec_id}/{qtd_de_repeticoes-1}")

    # ---------------------------------------------------
    # Loop nas queries
    # ---------------------------------------------------
    for sql_file in sql_files:
        query_name = os.path.basename(sql_file).replace(".sql", "")
        codigo_query = f"{codigo}_{query_name}_{tamanho_do_dado}_{exec_id}_de_{qtd_de_repeticoes-1}"
        log_filename = f"logs/spark_{query_name}_{timestamp_str}_{tamanho_do_dado}_{exec_id}_de_{qtd_de_repeticoes-1}.csv"
     
        monitor = SystemMonitor(
            output_file=log_filename,
            script_name=codigo_query,
            engine="spark"
        )
        monitor.start_monitoring()

        print(f"\n▶ Executando query: {query_name}")

        start_query = time.time()
        status = "success"

        # Ler SQL uma vez
        with open(sql_file, "r") as f:
            query_sql = f.read()

        try:
            # Loop nos parquets
            for file_path in parquet_files:
                file_name = os.path.basename(file_path)
                #print(f"  → Parquet: {file_name}")

                df = spark.read.parquet(file_path)
                df.createOrReplaceTempView("tabela")

                xlsx_path = f"resultados_xlsx/resultado_{codigo_query}.xlsx"
                pdf = spark.sql(query_sql).toPandas()
                pdf.to_excel(xlsx_path, index=False)

        except Exception as e:
            print(f"    ❌ Erro: {e}")
            print(f"parquet que deu erro:{file_path}")
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
                type="sql",
                description=query_name
            )
            monitor.stop_monitoring()

time.sleep(5)
spark.stop()


# no final, antes do print final
end_total = time.time()
tempo_total = end_total - start_total
minutos = int(tempo_total // 60)
segundos = int(tempo_total % 60)

print(f"\n⏱ Tempo total de execução do script: {minutos} min {segundos} s")
print(f"spark_incremental_{tamanho_do_dado} - Todas as consultas finalizadas.")
