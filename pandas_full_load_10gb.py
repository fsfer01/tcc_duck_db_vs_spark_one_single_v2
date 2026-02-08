import time
import os
import sys
import signal
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
import pandas as pd
import gc
import psutil

from src.utils import SystemMonitor, registrar_execucao

def limpar_memoria():
    """Força limpeza da memória antes de cada execução"""
    print("🧹 Limpando memória...")
    
    # 1. Coletor de lixo do Python
    gc.collect()
    
    # 2. Limpar cache do pandas
    pd.DataFrame().empty  # Truque para limpar cache interno

# ===================================================
# FUNÇÕES DAS CONSULTAS (substituem os SQLs)
# ===================================================
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

# ===================================================
# WORKER GLOBAL (para multiprocessing)
# ===================================================
def pandas_worker(parquet_files, consulta_name, queue):
    """Worker isolado - deve ser função global"""
    try:
        # Carregar todos parquets
        dataframes = []
        for file_path in parquet_files:
            df = pd.read_parquet(file_path)
            dataframes.append(df)
        
        # Concatenar tudo
        df_completo = pd.concat(dataframes, ignore_index=True)
        
        # Executar consulta
        if consulta_name == "consulta_1_count_lines":
            result = consulta_1(df_completo)
        elif consulta_name == "consulta_2_agreg_sample":
            result = consulta_2(df_completo)
        elif consulta_name == "consulta_3_agreg_columns":
            result = consulta_3(df_completo)
        else:
            queue.put(('error', f"Consulta desconhecida: {consulta_name}"))
            return
        
        # Materializar
        _ = result.values.tolist()
        queue.put(('success', result))
        
    except Exception as e:
        queue.put(('error', str(e)))

# ===================================================
# MAIN
# ===================================================
if __name__ == "__main__":
    # Configuração
    data_atual = (datetime.now() - timedelta(hours=3)).strftime("%Y-%m-%d %Hh:%Mmin")
    codigo = "pandas_full_load"
    tamanho_do_dado = "10_gb"
    start_total = time.time()

    # Caminhos
    parquet_dir = "/media/sf_HD_EXTERNO/bucket/10_gb/parquets/"
    parquet_files = [os.path.join(parquet_dir, f) for f in os.listdir(parquet_dir) 
                     if f.endswith(".parquet")]
    
    if not parquet_files:
        raise RuntimeError("Nenhum parquet encontrado")
    
    print(f"📁 Encontrados {len(parquet_files)} arquivos parquet")
  
    qtd_de_repeticoes = 15
    exec_inicio = 12  # retomar da 12ª execução

    for exec_id in range(exec_inicio, qtd_de_repeticoes + 1):
        limpar_memoria()

        print(f"\n{'='*60}")
        print(f"🚀 EXECUÇÃO {exec_id} DE {qtd_de_repeticoes}")
        print(f"{'='*60}")

        # ============ MUDANÇA AQUI ============
        # Criar log único para esta execução completa
        codigo_exec = f"{codigo}_{tamanho_do_dado}_{exec_id}_de_{qtd_de_repeticoes}"
        log_filename = f"logs/{codigo_exec}.csv"
        os.makedirs("logs", exist_ok=True)
        
        monitor = SystemMonitor(
            output_file=log_filename,
            script_name=codigo_exec,
            engine="python"
        )
        monitor.start_monitoring()
        # ======================================

        # LOOP CONSULTAS
        for consulta_name in CONSULTAS.keys():
            limpar_memoria()
            codigo_query = f"{codigo}_{consulta_name}_{tamanho_do_dado}_{exec_id}_de_{qtd_de_repeticoes}"
            xlsx_path = f"resultados_xlsx/resultado_{codigo_query}.xlsx"
            os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)
            start_query = time.time()
            status = "success"

            try:
                # Processo isolado
                queue = Queue()
                p = Process(
                    target=pandas_worker,
                    args=(parquet_files, consulta_name, queue)
                )

                p.start()
                p.join()

                # Verificar resultado
                if p.exitcode == 0:
                    if queue.empty():
                        print("❌ Processo terminou sem resultado")
                        status = "error"
                    else:
                        result_type, result_value = queue.get()
                        if result_type == 'success':
                            print(f"✅ Sucesso")
                            result_value.to_excel(xlsx_path, index=False)
                        else:
                            print(f"❌ Erro: {result_value}")
                            status = "error"

                elif p.exitcode in (-signal.SIGKILL, -9, 137):
                    print("💀 Worker morto por OOM / SIGKILL")
                    status = "killed"

                else:
                    print(f"❌ Exit code {p.exitcode}")
                    status = "error"

            except Exception as e:
                print(f"💥 Erro: {e}")
                status = "error"

            finally:
                # Registrar cada consulta individualmente
                registrar_execucao(
                    data_atual=data_atual,
                    codigo=codigo_query,
                    tamanho_do_dado=tamanho_do_dado,
                    start=start_query,
                    end=time.time(),
                    status=status,
                    type="sql",
                    description=consulta_name
                )
            
            time.sleep(1)

        # ============ MUDANÇA AQUI ============
        # Parar monitoramento APÓS todas as consultas
        monitor.stop_monitoring()
        # ======================================

    # Finalização
    end_total = time.time()
    tempo_total = end_total - start_total
    minutos = int(tempo_total // 60)
    segundos = int(tempo_total % 60)
    
    print(f"\n{'='*60}")
    print(f"⏱ Tempo total: {minutos} min {segundos} s")
    print(f"{codigo}_{tamanho_do_dado} - Finalizado")
    print(f"{'='*60}")