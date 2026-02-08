import pandas as pd
import time
import os
import glob
from datetime import datetime, timedelta
from src.utils import SystemMonitor, registrar_execucao

# ---------------------------------------------------
# FUNÇÕES DAS CONSULTAS
# ---------------------------------------------------
def consulta_1(df: pd.DataFrame):
    return pd.DataFrame({
        "count_total": [len(df)],
        "count_line": [len(df)]
    })

def consulta_2(df: pd.DataFrame):
    df = df.copy()
    df['reference_date'] = pd.to_datetime(df['request_datetime'], errors='coerce').dt.date
    return df.groupby('reference_date').agg(
        total_trips=('request_datetime', 'count'),
        sum_driver_pay=('driver_pay', 'sum'),
        sum_base_passenger_fare=('base_passenger_fare', 'sum'),
        sum_tips=('tips', 'sum'),
        sum_tolls=('tolls', 'sum'),
        sum_bcf=('bcf', 'sum'),
        sum_sales_tax=('sales_tax', 'sum'),
        sum_congestion_surcharge=('congestion_surcharge', 'sum'),
        sum_airport_fee=('airport_fee', 'sum'),
        sum_trip_miles=('trip_miles', 'sum'),
        avg_trip_miles=('trip_miles', 'mean'),
        sum_trip_time_seconds=('trip_time', 'sum'),
        avg_trip_time_seconds=('trip_time', 'mean')
    ).reset_index()

def consulta_3(df: pd.DataFrame):
    df = df.copy()
    df['reference_date'] = pd.to_datetime(df['request_datetime'], errors='coerce').dt.date
    result = df.groupby('reference_date').agg(
        cnt_hvfhs_license_num=('hvfhs_license_num', pd.Series.nunique),
        cnt_dispatching_base_num=('dispatching_base_num', pd.Series.nunique),
        cnt_originating_base_num=('originating_base_num', pd.Series.nunique),
        cnt_pu_location_id=('pu_location_id', pd.Series.nunique),
        cnt_do_location_id=('do_location_id', pd.Series.nunique),
        total_trips=('request_datetime', 'count'),
        sum_driver_pay=('driver_pay', 'sum'),
        sum_base_passenger_fare=('base_passenger_fare', 'sum'),
        sum_tips=('tips', 'sum'),
        sum_tolls=('tolls', 'sum'),
        sum_bcf=('bcf', 'sum'),
        sum_sales_tax=('sales_tax', 'sum'),
        sum_congestion_surcharge=('congestion_surcharge', 'sum'),
        sum_airport_fee=('airport_fee', 'sum'),
        sum_trip_miles=('trip_miles', 'sum'),
        avg_trip_miles=('trip_miles', 'mean'),
        sum_trip_time_seconds=('trip_time', 'sum'),
        avg_trip_time_seconds=('trip_time', 'mean'),
        cnt_shared_request_flag=('shared_request_flag', 'count'),
        cnt_shared_match_flag=('shared_match_flag', 'count'),
        cnt_access_a_ride_flag=('access_a_ride_flag', 'count'),
        cnt_wav_request_flag=('wav_request_flag', 'count'),
        cnt_wav_match_flag=('wav_match_flag', 'count')
    ).reset_index()
    return result.sort_values("reference_date").reset_index(drop=True)

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
tamanho_do_dado = '10_gb'
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
    print(f"\n🚀 Iniciando execução {exec_id}/{qtd_de_repeticoes-1}")

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