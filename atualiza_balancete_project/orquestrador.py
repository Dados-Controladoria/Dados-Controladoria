# orquestrador.py
import subprocess
import logging
import sys
import time
import os

# Define o caminho base onde o script está localizado
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Configuração do Logging
logging.basicConfig(
    filename=os.path.join(BASE_DIR, 'processo_orquestrador.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filemode='a',
    encoding='utf-8'
)

# Funções visuais (sem alterações)
def exibir_banner():
    """Exibe um banner ASCII no terminal."""
    print("=" * 70)
    print(r"""
 
          ██╗  ██╗ ██████╗ ██████╗ ██╗   ██╗███████╗
          ██║  ██║██╔═══██╗██╔══██╗██║   ██║██╔════╝
          ███████║██║   ██║██████╔╝██║   ██║███████╗
          ██╔══██║██║   ██║██╔══██╗██║   ██║╚════██║
          ██║  ██║╚██████╔╝██║  ██║╚██████╔╝███████║
          ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚══════╝
                                         
              >> ORQUESTRADOR DE ETL v1.0 <<
    """)
    print("=" * 70)
    time.sleep(1)

def atualizar_barra_progresso(progresso, total, comprimento=50):
    """Desenha ou atualiza uma barra de progresso no terminal."""
    percentual = int((progresso / total) * 100)
    blocos_preenchidos = int(comprimento * progresso // total)
    barra = "█" * blocos_preenchidos + "-" * (comprimento - blocos_preenchidos)
    sys.stdout.write(f"\rProgresso: |{barra}| {percentual}% Completo")
    sys.stdout.flush()

# Função para Executar Processos Externos (sem alterações)
def executar_processo(comando):
    try:
        logging.info(f"Executando comando: {' '.join(comando)}")
        resultado = subprocess.run(comando, check=True, capture_output=True, text=True, encoding='utf-8', cwd=BASE_DIR)
        if resultado.stdout: logging.info("Saida do processo:\n" + resultado.stdout)
        if resultado.stderr: logging.warning("Saida de erro do processo:\n" + resultado.stderr)
        logging.info("Comando executado com sucesso.")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"!!! ERRO ao executar o comando: {' '.join(comando)}")
        logging.error("Codigo de retorno: " + str(e.returncode))
        if e.stdout: logging.error("Saida do processo (stdout):\n" + e.stdout)
        if e.stderr: logging.error("Saida de erro (stderr):\n" + e.stderr)
        return False
    except FileNotFoundError:
        logging.error(f"!!! ERRO CRÍTICO: O comando '{comando[0]}' nao foi encontrado.")
        logging.error("Verifique se o Python está no PATH do sistema.")
        return False

# Bloco Principal de Orquestração
if __name__ == "__main__":
    exibir_banner()

    passo_atual = 0
    total_passos = 3
    status_final = "FALHA" # Define um status padrão

    try:
        logging.info("================ INICIO DA ORQUESTRACAO (HORUS) ================")
        atualizar_barra_progresso(passo_atual, total_passos)

        # ETAPA 1: Epoca
        logging.info("--> ETAPA 1 de 3: Executando processador ETL da Epoca...")
        script_epoca = os.path.join(BASE_DIR, "processador_ETL_epoca.py")
        # ==============================================================================
        # --- ALTERAÇÃO: Usa sys.executable em vez de "python" ---
        sucesso_epoca = executar_processo([sys.executable, script_epoca])
        # ==============================================================================
        if sucesso_epoca:
            passo_atual += 1
            atualizar_barra_progresso(passo_atual, total_passos)
        else:
            raise Exception("Falha na Etapa 1: ETL Epoca")

        # ETAPA 2: Magalu
        logging.info("--> ETAPA 2 de 3: Executando processador ETL do Magalu...")
        script_magalu = os.path.join(BASE_DIR, "processador_ETL_magalu.py")
        # ==============================================================================
        # --- ALTERAÇÃO: Usa sys.executable em vez de "python" ---
        sucesso_magalu = executar_processo([sys.executable, script_magalu])
        # ==============================================================================
        if sucesso_magalu:
            passo_atual += 1
            atualizar_barra_progresso(passo_atual, total_passos)
        else:
            raise Exception("Falha na Etapa 2: ETL Magalu")
            
        # ETAPA 3: Notebook
        logging.info("--> ETAPA 3 de 3: Executando notebook de integracao...")
        notebook_integracao = os.path.join(BASE_DIR, "integracao_sheets.ipynb")
        # Para o jupyter, mantemos o comando original, pois ele é um programa separado.
        comando_notebook = ["jupyter", "nbconvert", "--to", "notebook", "--execute", notebook_integracao, "--inplace"]
        sucesso_notebook = executar_processo(comando_notebook)
        if sucesso_notebook:
            passo_atual += 1
            atualizar_barra_progresso(passo_atual, total_passos)
            print("\n\nOrquestração concluída com SUCESSO!")
            logging.info("--- PROCESSO DE INTEGRACAO CONCLUIDO COM SUCESSO! ---")
            status_final = "SUCESSO" # Atualiza o status
        else:
            raise Exception("Falha na Etapa 3: Notebook de Integração")

    except KeyboardInterrupt:
        print("\n\nProcesso interrompido pelo usuário.")
        logging.warning("!!! ORQUESTRACAO INTERROMPIDA PELO USUARIO !!!")
        status_final = "INTERROMPIDO"
        sys.exit(1)
        
    except Exception as e:
        print(f"\n\nERRO: A orquestração falhou. Verifique o arquivo 'processo_orquestrador.log' para detalhes.")
        logging.error(f"!!! ORQUESTRAÇÃO FALHOU: {e} !!!")
        sys.exit(1)

    finally:
        logging.info(f"================ FIM DA ORQUESTRACAO (Status: {status_final}) ================\n")