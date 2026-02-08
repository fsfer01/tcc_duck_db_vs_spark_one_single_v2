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
codigo = "spark_full_load"
tamanho_do_dado = "100GB"

# tempo total do script
start_total = time.time()

# ---------------------------------------------------
# CONFIG Spark
# ---------------------------------------------------
spark = (
    SparkSession.builder
        .appName("spark-metrics-test")

        # Adaptive Query Execution (correto manter)
        .config("spark.sql.adaptive.enabled", "true")

        # Memória (ok, mas não resolve disco)
        .config("spark.driver.memory", "16g")
        .config("spark.executor.memory", "16g")

        # 🔴 ESSENCIAL: diretório de spill/shuffle
        .config("spark.local.dir", "/home/ferreirinha/Área de trabalho/tcc_duck_db_vs_spark_one_single/spark_tmp")

        # Reduz pressão de memória durante shuffle
        .config("spark.sql.shuffle.partitions", "200")

        .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------
# Caminhos
# ---------------------------------------------------
parquet_path = "/media/sf_HD_EXTERNO/bucket/100_gb/parquets/*.parquet"
sql_folder = "/media/sf_HD_EXTERNO/bucket/100_gb/consultas_sql/*.sql"

# validações (opcional, só para segurança)
parquet_files = glob.glob(parquet_path)
if not parquet_files:
    raise RuntimeError("Nenhum parquet encontrado")

sql_files = sorted(glob.glob(sql_folder))
if not sql_files:
    raise RuntimeError("Nenhum SQL encontrado")

timestamp_str = (datetime.now() - timedelta(hours=3)).strftime("%Y_%m_%d")

# ---------------------------------------------------
# Leitura dos parquets (UMA VEZ)
# ---------------------------------------------------
print("\n📥 Lendo todos os parquets em um único DataFrame Spark...")
df = spark.read.parquet(parquet_path)
df.createOrReplaceTempView("tabela")

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
            xlsx_path = f"resultados_xlsx/resultado_{codigo_query}.xlsx"
            pdf = spark.sql(query_sql).toPandas()
            pdf.to_excel(xlsx_path, index=False)

        except Exception as e:
            print(f"    ❌ Erro: {e}")
            status = "error"

        finally:
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

# ---------------------------------------------------
# Finalização
# ---------------------------------------------------
time.sleep(5)
spark.stop()
end_total = time.time()
tempo_total = end_total - start_total

minutos = int(tempo_total // 60)
segundos = int(tempo_total % 60)

print(f"\n⏱ Tempo total de execução do script: {minutos} min {segundos} s")
print(f"spark_full_load_{tamanho_do_dado} - Todas as consultas finalizadas.")
