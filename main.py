import multiprocessing
import threading
import time
import random
import os
import sys
from dataclasses import dataclass
import collections


# --- Estrutura do Trabalho de Impressão ---
@dataclass
class TrabalhoImpressao:
    id_job: int
    nome_arquivo: str
    numero_paginas: int


# --- Constantes de Configuração ---
NUM_CLIENTES = 10
NUM_IMPRESSORAS = 5
JOBS_POR_CLIENTE = 2
LOG_FILE = "log_servidor.txt"


# ==============================================================================
# 1. Lógica das Threads (Impressoras) - COM LOCK EXPLÍCITO (Mantido)
# ==============================================================================
def printer_worker(thread_id: int, job_queue: collections.deque, condition: threading.Condition,
                   log_queue: multiprocessing.Queue):
    """
    Função da impressora usando um lock explícito e uma variável de condição.
    """
    log_queue.put(f"[Impressora {thread_id}] Online e pronta.")
    while True:
        job = None

        # --- SEÇÃO CRÍTICA INICIA AQUI ---
        with condition:  # 'with' adquire e libera o lock automaticamente
            # Espera enquanto a fila estiver vazia E o sinal de parada não for o job atual
            while not job_queue:
                # Espera por uma notificação. O lock é liberado enquanto espera.
                condition.wait()

                # Quando acordar, o lock é readquirido. Pega o trabalho.
            job = job_queue.popleft()
        # --- SEÇÃO CRÍTICA TERMINA AQUI ---

        if job is None:
            log_queue.put(f"[Impressora {thread_id}] Recebeu sinal de desligamento. Encerrando.")
            break

        log_queue.put(
            f"[Impressora {thread_id}] Começou a imprimir Job #{job.id_job} ({job.nome_arquivo}, {job.numero_paginas} pág.)")
        time.sleep(job.numero_paginas * 0.1)
        log_queue.put(f"[Impressora {thread_id}] Concluiu a impressão do Job #{job.id_job}")

    log_queue.put(f"[Impressora {thread_id}] Offline.")


# ==============================================================================
# 2. Lógica do Processo Servidor (Gerenciador de Impressão) - (Mantido)
# ==============================================================================
def log_manager(log_queue: multiprocessing.Queue):
    with open(LOG_FILE, "w") as f:
        f.write("--- Início do Log do Servidor de Impressão ---\n")
    while True:
        message = log_queue.get()
        if message is None:
            break
        with open(LOG_FILE, "a") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")


def servidor_process(ipc_queue: multiprocessing.Queue):
    print(f"[Servidor PID: {os.getpid()}] Iniciado.")

    job_queue = collections.deque()
    lock = threading.Lock()
    condition = threading.Condition(lock)

    log_queue = multiprocessing.Queue()
    log_thread = threading.Thread(target=log_manager, args=(log_queue,))
    log_thread.start()
    log_queue.put(f"[Servidor] Servidor de impressão iniciado com {NUM_IMPRESSORAS} impressora(s).")

    impressoras = []
    for i in range(NUM_IMPRESSORAS):
        thread = threading.Thread(target=printer_worker, args=(i + 1, job_queue, condition, log_queue))
        thread.start()
        impressoras.append(thread)

    total_jobs_esperados = NUM_CLIENTES * JOBS_POR_CLIENTE
    jobs_recebidos = 0
    while jobs_recebidos < total_jobs_esperados:
        job: TrabalhoImpressao = ipc_queue.get()
        log_queue.put(f"[Servidor] Recebido Job #{job.id_job} ('{job.nome_arquivo}') do cliente.")

        with condition:
            job_queue.append(job)
            condition.notify()
        jobs_recebidos += 1

    log_queue.put("[Servidor] Todos os trabalhos foram recebidos dos clientes.")

    while True:
        with lock:
            if not job_queue:
                break
        time.sleep(0.1)

    log_queue.put("[Servidor] Fila de impressão vazia. Desligando as impressoras...")

    with condition:
        for _ in impressoras:
            job_queue.append(None)
        condition.notify_all()

    for thread in impressoras:
        thread.join()

    log_queue.put(None)
    log_thread.join()
    print(f"[Servidor PID: {os.getpid()}] Encerrado.")


# ==============================================================================
# 3. Lógica do Processo Cliente - (Mantido)
# ==============================================================================
def cliente_process(client_id: int, ipc_queue: multiprocessing.Queue):
    print(f"[Cliente {client_id} PID: {os.getpid()}] Iniciado.")
    for i in range(JOBS_POR_CLIENTE):
        job = TrabalhoImpressao(
            id_job=client_id * 100 + i,
            nome_arquivo=f"doc_cliente_{client_id}_job_{i}.pdf",
            numero_paginas=random.randint(1, 10)
        )
        print(f"[Cliente {client_id}] Enviando Job #{job.id_job} ({job.nome_arquivo}) para o servidor.")
        ipc_queue.put(job)
        time.sleep(random.uniform(0.1, 0.5))
    print(f"[Cliente {client_id} PID: {os.getpid()}] Todos os trabalhos foram enviados.")


# ==============================================================================
# 4. Orquestração Principal - REVERTIDO PARA multiprocessing.Process
# ==============================================================================
if __name__ == "__main__":
    print("--- Iniciando Simulação do Spooler de Impressão ---")

    # A fila de comunicação entre processos funciona em todas as plataformas.
    fila_ipc = multiprocessing.Queue()

    # 1. Inicia o processo do servidor usando a classe Process
    processo_servidor = multiprocessing.Process(target=servidor_process, args=(fila_ipc,))
    processo_servidor.start()

    # 2. Inicia os processos dos clientes usando a classe Process
    processos_clientes = []
    for i in range(NUM_CLIENTES):
        processo_cliente = multiprocessing.Process(target=cliente_process, args=(i + 1, fila_ipc))
        processos_clientes.append(processo_cliente)
        processo_cliente.start()

    # 3. Espera todos os clientes terminarem com .join()
    for p in processos_clientes:
        p.join()

    # 4. Espera o servidor processar tudo e encerrar com .join()
    processo_servidor.join()

    print("\n--- Simulação Concluída ---")
    print(f"Verifique o arquivo '{LOG_FILE}' para o registro de atividades.")