import multiprocessing
import threading
import time
import random
import os
from queue import Queue, Empty
from dataclasses import dataclass


# --- Estrutura do Trabalho de Impressão ---
@dataclass
class TrabalhoImpressao:
    id_job: int
    nome_arquivo: str
    numero_paginas: int


# --- Constantes de Configuração ---
NUM_CLIENTES = 3
NUM_IMPRESSORAS = 2
JOBS_POR_CLIENTE = 2
LOG_FILE = "log_servidor.txt"


# ==============================================================================
# 1. Lógica das Threads (Impressoras)
# ==============================================================================
def printer_worker(thread_id: int, job_queue: Queue, log_queue: Queue):
    """
    Função executada por cada thread de impressora.
    Consome trabalhos da fila e simula a impressão.
    """
    log_queue.put(f"[Impressora {thread_id}] Online e pronta.")
    while True:
        try:
            # Pega um trabalho da fila. O 'block=True' faz a thread esperar
            # se a fila estiver vazia. O timeout evita bloqueio infinito.
            job = job_queue.get(block=True, timeout=1)

            # Condição de parada: se o job for None, a thread deve terminar.
            if job is None:
                log_queue.put(f"[Impressora {thread_id}] Recebeu sinal de desligamento. Encerrando.")
                break

            log_queue.put(
                f"[Impressora {thread_id}] Começou a imprimir Job #{job.id_job} ({job.nome_arquivo}, {job.numero_paginas} pág.)")

            # Simula o tempo de impressão (0.1 segundo por página)
            time.sleep(job.numero_paginas * 0.1)

            log_queue.put(f"[Impressora {thread_id}] Concluiu a impressão do Job #{job.id_job}")
            job_queue.task_done()  # Sinaliza que o trabalho foi concluído

        except Empty:
            # Se a fila continuar vazia após o timeout, continua esperando.
            # Isso é útil para quando não há mais jobs mas o servidor ainda não mandou o sinal de parada.
            continue
    log_queue.put(f"[Impressora {thread_id}] Offline.")


# ==============================================================================
# 2. Lógica do Processo Servidor (Gerenciador de Impressão)
# ==============================================================================
def log_manager(log_queue: Queue):
    """
    Uma thread dedicada para escrever no arquivo de log, evitando concorrência de I/O.
    """
    with open(LOG_FILE, "w") as f:
        f.write("--- Início do Log do Servidor de Impressão ---\n")

    while True:
        message = log_queue.get()
        if message is None:  # Sinal de parada
            break

        with open(LOG_FILE, "a") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")


def servidor_process(ipc_queue: multiprocessing.Queue):
    """
    Processo principal do servidor. Recebe trabalhos dos clientes,
    gerencia a fila de impressão e as threads das impressoras.
    """
    print(f"[Servidor PID: {os.getpid()}] Iniciado.")

    # Fila interna para threads (impressoras) - é uma fila thread-safe
    job_queue = Queue()

    # Fila para centralizar as mensagens de log
    log_queue = Queue()

    # Inicia a thread de gerenciamento de log
    log_thread = threading.Thread(target=log_manager, args=(log_queue,))
    log_thread.start()

    log_queue.put(f"[Servidor] Servidor de impressão iniciado com {NUM_IMPRESSORAS} impressora(s).")

    # Cria e inicia o pool de threads (impressoras)
    impressoras = []
    for i in range(NUM_IMPRESSORAS):
        thread = threading.Thread(target=printer_worker, args=(i + 1, job_queue, log_queue))
        thread.start()
        impressoras.append(thread)

    # Loop para receber trabalhos dos clientes via IPC
    total_jobs_esperados = NUM_CLIENTES * JOBS_POR_CLIENTE
    jobs_recebidos = 0
    while jobs_recebidos < total_jobs_esperados:
        job: TrabalhoImpressao = ipc_queue.get()
        log_queue.put(f"[Servidor] Recebido Job #{job.id_job} ('{job.nome_arquivo}') do cliente.")
        job_queue.put(job)  # Adiciona o trabalho à fila das impressoras
        jobs_recebidos += 1

    log_queue.put("[Servidor] Todos os trabalhos foram recebidos dos clientes.")

    # Espera até que todos os trabalhos na fila tenham sido processados
    job_queue.join()
    log_queue.put("[Servidor] Fila de impressão vazia. Desligando as impressoras...")

    # Envia o sinal de parada (None) para cada thread de impressora
    for _ in impressoras:
        job_queue.put(None)

    # Espera todas as threads de impressora terminarem
    for thread in impressoras:
        thread.join()

    # Envia o sinal de parada para a thread de log e espera ela terminar
    log_queue.put(None)
    log_thread.join()

    print(f"[Servidor PID: {os.getpid()}] Encerrado.")


# ==============================================================================
# 3. Lógica do Processo Cliente
# ==============================================================================
def cliente_process(client_id: int, ipc_queue: multiprocessing.Queue):
    """
    Processo cliente que gera e envia trabalhos de impressão para o servidor.
    """
    print(f"[Cliente {client_id} PID: {os.getpid()}] Iniciado.")
    for i in range(JOBS_POR_CLIENTE):
        # Gera um trabalho de impressão com dados aleatórios
        job = TrabalhoImpressao(
            id_job=client_id * 100 + i,  # ID único para o job
            nome_arquivo=f"doc_cliente_{client_id}_job_{i}.pdf",
            numero_paginas=random.randint(1, 10)
        )
        print(f"[Cliente {client_id}] Enviando Job #{job.id_job} ({job.nome_arquivo}) para o servidor.")
        ipc_queue.put(job)
        time.sleep(random.uniform(0.1, 0.5))  # Simula um intervalo entre envios

    print(f"[Cliente {client_id} PID: {os.getpid()}] Todos os trabalhos foram enviados.")


# ==============================================================================
# 4. Orquestração Principal
# ==============================================================================
if __name__ == "__main__":
    print("--- Iniciando Simulação do Spooler de Impressão ---")

    # Cria a fila de comunicação entre processos (IPC)
    # multiprocessing.Queue é segura para uso entre processos.
    fila_ipc = multiprocessing.Queue()

    # 1. Inicia o processo do servidor
    processo_servidor = multiprocessing.Process(target=servidor_process, args=(fila_ipc,))
    processo_servidor.start()

    # 2. Inicia os processos dos clientes
    processos_clientes = []
    for i in range(NUM_CLIENTES):
        processo_cliente = multiprocessing.Process(target=cliente_process, args=(i + 1, fila_ipc))
        processos_clientes.append(processo_cliente)
        processo_cliente.start()

    # 3. Espera todos os clientes terminarem de enviar seus trabalhos
    for p in processos_clientes:
        p.join()

    # 4. Espera o servidor processar tudo e encerrar
    processo_servidor.join()

    print("\n--- Simulação Concluída ---")
    print(f"Verifique o arquivo '{LOG_FILE}' para o registro de atividades.")
