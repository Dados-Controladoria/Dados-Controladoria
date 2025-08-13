# orquestrador.py
import subprocess
import logging
import sys
import time
import os
import locale
import threading
import glob

# --- Configuração Inicial ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)
PROJECT_DIR = os.path.dirname(BASE_DIR)
logging.basicConfig(
    filename=os.path.join(PROJECT_DIR, 'processo_orquestrador.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filemode='a',
    encoding='utf-8'
)

# --- Funções ---
def exibir_banner():
    print("=" * 70)
    print(r"""
 
          ██╗  ██╗ ██████╗ ██████╗ ██╗   ██╗███████╗
          ██║  ██║██╔═══██╗██╔══██╗██║   ██║██╔════╝
          ███████║██║   ██║██████╔╝██║   ██║███████╗
          ██╔══██║██║   ██║██╔══██╗██║   ██║╚════██║
          ██║  ██║╚██████╔╝██║  ██║╚██████╔╝███████║
          ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚══════╝
                                         
              >> ORQUESTRADOR DE ETL v1.5 <<
    """)
    print("=" * 70)
    time.sleep(1)

def atualizar_status_linha(progresso, total, comprimento=50, char_spinner=''):
    percentual = int((progresso / total) * 100)
    blocos_preenchidos = int(comprimento * progresso // total)
    barra = "█" * blocos_preenchidos + "-" * (comprimento - blocos_preenchidos)
    linha_status = f"Progresso Geral: |{barra}| {percentual}% [{char_spinner}]"
    sys.stdout.write(f"\r{linha_status:<80}")
    sys.stdout.flush()

def stream_output_reader(pipe, log_level):
    try:
        for line in iter(pipe.readline, ''):
            line = line.strip()
            if line:
                sys.stdout.write("\r" + " " * 80 + "\r")
                print(f"    > {line}")
                if log_level == 'info': logging.info(line)
                else: logging.warning(line)
        pipe.close()
    except Exception as e:
        logging.error(f"Erro no leitor de stream: {e}")

def executar_com_progresso(descricao_etapa, comando, passo_atual, total_passos):
    print("\n" + "=" * 70)
    print(f"  {descricao_etapa}")
    print("=" * 70)
    logging.info(descricao_etapa)
    try:
        process = subprocess.Popen(
            comando, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding=locale.getpreferredencoding(False),
            errors='replace', cwd=BASE_DIR
        )
        stdout_thread = threading.Thread(target=stream_output_reader, args=(process.stdout, 'info'))
        stderr_thread = threading.Thread(target=stream_output_reader, args=(process.stderr, 'warning'))
        stdout_thread.start()
        stderr_thread.start()
        spinner_chars = ['/', '-', '\\', '|']
        i = 0
        print(f"[Iniciando etapa {passo_atual+1}/{total_passos}] {descricao_etapa}...")
        process.wait()  # aguarda terminar
        stdout_thread.join()
        stderr_thread.join()
        if process.returncode == 0:
            atualizar_status_linha(passo_atual + 1, total_passos, char_spinner='OK')
            print("\n" + "-" * 70)
            print(f"  ETAPA CONCLUIDA COM SUCESSO")
            print("-" * 70)
            logging.info(f"Etapa '{descricao_etapa}' concluída com sucesso.")
            return True
        else:
            sys.stdout.write("\r" + " " * 80 + "\r")
            logging.error(f"!!! Etapa '{descricao_etapa}' falhou com código de retorno {process.returncode}.")
            return False
    except Exception:
        sys.stdout.write("\r" + " " * 80 + "\r")
        logging.error(f"!!! ERRO INESPERADO na execução da etapa '{descricao_etapa}'", exc_info=True)
        return False

# --- Bloco Principal ---
if __name__ == "__main__":
    exibir_banner()
    
    total_passos = 3
    passo_atual = 0
    status_final = "FALHA"

    try:
        logging.info("================ INICIO DA ORQUESTRACAO (HORUS) ================")
        atualizar_status_linha(passo_atual, total_passos)
        
        # ETAPA 1: Epoca
        if not executar_com_progresso(f"ETAPA 1/{total_passos}: Processador ETL da Epoca", [sys.executable, "processador_ETL_epoca.py"], passo_atual, total_passos):
            raise Exception("Falha na Etapa 1: ETL Epoca")
        passo_atual += 1

        # ETAPA 2: Magalu
        if not executar_com_progresso(f"ETAPA 2/{total_passos}: Processador ETL do Magalu", [sys.executable, "processador_ETL_magalu.py"], passo_atual, total_passos):
            raise Exception("Falha na Etapa 2: ETL Magalu")
        passo_atual += 1
            
        # ETAPA 3: Notebook
        # Procura automaticamente o .ipynb na pasta src
        notebooks = glob.glob(os.path.join(BASE_DIR, "*.ipynb"))
        if not notebooks:
            raise FileNotFoundError(f"Nenhum arquivo .ipynb encontrado em {BASE_DIR}")
        
        caminho_absoluto_notebook = notebooks[0]
        logging.info(f"Notebook encontrado: {caminho_absoluto_notebook}")

        comando_notebook = [
            "jupyter", "nbconvert", "--to", "notebook", "--execute",
            caminho_absoluto_notebook, "--inplace"
        ]

        if not executar_com_progresso(f"ETAPA 3/{total_passos}: Integracao com Google Sheets", comando_notebook, passo_atual, total_passos):
            raise Exception("Falha na Etapa 3: Notebook de Integração")
        
        print("\n\n" + "="*25 + " ORQUESTRACAO FINALIZADA " + "="*26)
        print("Todos os processos foram concluidos com sucesso!")
        status_final = "SUCESSO"

    except KeyboardInterrupt:
        print("\n\nProcesso interrompido pelo usuário.")
        logging.warning("!!! ORQUESTRACAO INTERROMPIDA PELO USUARIO !!!")
        status_final = "INTERROMPIDO"
        sys.exit(1)
        
    except Exception as e:
        print(f"\n\nERRO: A orquestração falhou. Verifique o arquivo 'processo_orquestrador.log' para detalhes.")
        logging.error(f"!!! ORQUESTRAÇÃO FALHOU: {e} !!!", exc_info=True)
        sys.exit(1)

    finally:
        logging.info(f"================ FIM DA ORQUESTRACAO (Status: {status_final}) ================\n")