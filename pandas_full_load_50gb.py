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
            limpar_memoria()
            result = consulta_1(df_completo)
        elif consulta_name == "consulta_2_agreg_sample":
            limpar_memoria()
            result = consulta_2(df_completo)
        elif consulta_name == "consulta_3_agreg_columns":
            limpar_memoria()
            result = consulta_3(df_completo)
        else:
            limpar_memoria()
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
    tamanho_do_dado = "50_gb"
    start_total = time.time()

    # Caminhos
    parquet_dir = f"/media/sf_HD_EXTERNO/bucket/{tamanho_do_dado}/parquets/"
    parquet_files = [os.path.join(parquet_dir, f) for f in os.listdir(parquet_dir) 
                     if f.endswith(".parquet")]
    
    if not parquet_files:
        raise RuntimeError("Nenhum parquet encontrado")
    
    print(f"📁 Encontrados {len(parquet_files)} arquivos parquet")
  
    qtd_de_repeticoes = 15
    
    # LOOP EXECUÇÕES
    for exec_id in range(1, qtd_de_repeticoes + 1):
        limpar_memoria()

        print(f"\n{'='*60}")
        print(f"🚀 EXECUÇÃO {exec_id} DE {qtd_de_repeticoes}")
        print(f"{'='*60}")

        # LOOP CONSULTAS
        for consulta_name in CONSULTAS.keys():
            limpar_memoria()
            codigo_query = f"{codigo}_{consulta_name}_{tamanho_do_dado}_{exec_id}_de_{qtd_de_repeticoes}"
            log_filename = f"logs/{codigo_query}.csv"
            xlsx_path = f"resultados_xlsx/resultado_{codigo_query}.xlsx"
            os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)

            
            os.makedirs("logs", exist_ok=True)

            print(f"\n▶ Consulta: {consulta_name}")
            print(f"   exec_id: {exec_id}")

            monitor = SystemMonitor(
                output_file=log_filename,
                script_name=codigo_query,
                engine="python"
            )
            monitor.start_monitoring()

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
                        limpar_memoria()
                    else:
                        result_type, result_value = queue.get()
                        if result_type == 'success':
                            limpar_memoria()
                            print(f"✅ Sucesso")
                            result_value.to_excel(xlsx_path, index=False)
                        else:
                            limpar_memoria()
                            print(f"❌ Erro: {result_value}")
                            status = "error"

                elif p.exitcode in (-signal.SIGKILL, -9, 137):
                    limpar_memoria()
                    print("💀 Worker morto por OOM / SIGKILL")
                    status = "killed"

                else:
                    limpar_memoria()
                    print(f"❌ Exit code {p.exitcode}")
                    status = "error"

            except Exception as e:
                limpar_memoria()
                print(f"💥 Erro: {e}")
                status = "error"

            finally:
                # Registrar
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

                monitor.stop_monitoring()
            
            time.sleep(1)

    # Finalização
    end_total = time.time()
    tempo_total = end_total - start_total
    minutos = int(tempo_total // 60)
    segundos = int(tempo_total % 60)
    
    print(f"\n{'='*60}")
    print(f"⏱ Tempo total: {minutos} min {segundos} s")
    print(f"{codigo}_{tamanho_do_dado} - Finalizado")
    print(f"{'='*60}")