import psutil
import time
import csv
import threading
import os
# logger_execucao.py
import logging
from typing import Optional
from datetime import timedelta
from typing import Optional
import logging

class SystemMonitor:
    def __init__(self, output_file, script_name, engine="python"):
        self.output_file = output_file
        self.script_name = script_name
        self.engine = engine
        self.running = False
        self.thread = None
        self.process = psutil.Process()

    # =========================
    # CGroup helpers (Docker)
    # =========================
    def get_container_memory_used_mb(self):
        try:
            with open("/sys/fs/cgroup/memory.current", "r") as f:
                return int(f.read()) / 1024 / 1024
        except Exception:
            return None

    def get_container_memory_limit_mb(self):
        try:
            with open("/sys/fs/cgroup/memory.max", "r") as f:
                value = f.read().strip()
                if value == "max":
                    return None
                return int(value) / 1024 / 1024
        except Exception:
            return None

    # =========================
    # Spark JVM (opcional)
    # =========================
    def get_spark_jvm_memory_mb(self):
        total = 0
        for p in psutil.process_iter(['name', 'cmdline', 'memory_info']):
            try:
                if (
                    p.info['name'] == 'java'
                    and p.info['cmdline']
                    and "org.apache.spark" in " ".join(p.info['cmdline'])
                ):
                    total += p.info['memory_info'].rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return total / 1024**2 if total > 0 else 0

    # =========================
    # Monitor loop
    # =========================
    def _monitor(self):
        with open(self.output_file, mode='w', newline='') as file:
            writer = csv.writer(file)

            writer.writerow([
                "timestamp",
                "cpu_usage_percent",
                "container_memory_used_mb",
                "container_memory_limit_mb",
                "container_memory_percent",
                "python_rss_mb",
                "spark_ram_usage_mb",
                "script_name"
            ])

            while self.running:
                start_time = time.time()
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

                cpu_usage = psutil.cpu_percent(interval=None)

                container_used = self.get_container_memory_used_mb()
                container_limit = self.get_container_memory_limit_mb()

                container_percent = (
                    (container_used / container_limit) * 100
                    if container_used and container_limit
                    else None
                )

                python_rss_mb = (
                    self.process.memory_info().rss / 1024 / 1024
                )

                spark_ram_usage_mb = 0
                if self.engine == "spark":
                    spark_ram_usage_mb = self.get_spark_jvm_memory_mb()

                writer.writerow([
                    timestamp,
                    cpu_usage,
                    container_used,
                    container_limit,
                    container_percent,
                    python_rss_mb,
                    spark_ram_usage_mb,
                    self.script_name
                ])

                file.flush()

                elapsed = time.time() - start_time
                time.sleep(max(1 - elapsed, 0))

    def start_monitoring(self):
        self.running = True
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()

    def stop_monitoring(self):
        self.running = False
        self.thread.join()


TIMEOUT_SECONDS = 10 * 60 * 60  # 2 horas

_logger_configurado = False


def _configurar_logger():
    global _logger_configurado

    if _logger_configurado:
        return

    logging.basicConfig(
        filename="benchmark_execucao.log",
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    _logger_configurado = True


def registrar_execucao(
    data_atual: str,
    codigo: str,
    tamanho_do_dado: str,
    start: float,
    end: Optional[float],
    status: str,
    type: str,
    description: str
) -> dict:
    """
    Registra tempo de execução em arquivo .log.
    """

    _configurar_logger()

    if end is None:
        tempo_de_exec = "timeout"
    else:
        duracao = end - start  # segundos

        if duracao > TIMEOUT_SECONDS:
            tempo_de_exec = "timeout"
        else:
            total_seconds = int(duracao)

            horas = total_seconds // 3600
            minutos = (total_seconds % 3600) // 60
            segundos = total_seconds % 60

            tempo_de_exec = f"{horas:02d}:{minutos:02d}:{segundos:02d}"

    status_final = status or "success"

    mensagem = (
        f"data={data_atual} | "
        f"codigo={codigo} | "
        f"tamanho_dado={tamanho_do_dado} | "
        f"tempo_exec={tempo_de_exec} | "
        f"status={status_final} | "
        f"type={type} | "
        f"description={description}"
    )

    logging.info(mensagem)

    return {
        "data_atual": data_atual,
        "codigo": codigo,
        "tamanho_do_dado": tamanho_do_dado,
        "tempo_de_exec": tempo_de_exec,
        "status": status_final,
        "type": type,
        "description": description
    }