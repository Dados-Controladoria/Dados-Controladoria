# Dentro de orquestrador.py
import subprocess
import logging

# Configuração do logging
logging.basicConfig(filename='processo.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def executar_processo(comando):
    """Executa um comando no shell e loga a saída."""
    try:
        logging.info(f"Executando comando: {' '.join(comando)}")
        resultado = subprocess.run(comando, check=True, capture_output=True, text=True)
        logging.info("Saída do processo:\n" + resultado.stdout)
        if resultado.stderr:
            logging.warning("Saída de erro do processo:\n" + resultado.stderr)
        logging.info("Comando executado com sucesso.")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Erro ao executar o comando: {' '.join(comando)}")
        logging.error("Código de retorno: " + str(e.returncode))
        logging.error("Saída:\n" + e.stdout)
        logging.error("Erro:\n" + e.stderr)
        return False
    except FileNotFoundError:
        logging.error(f"Erro: O script '{comando[1]}' não foi encontrado.")
        return False

if __name__ == "__main__":
    logging.info("--- Início da orquestração dos ETLs ---")
    
    # --- Processar Balancete Magalu ---
    # (Supondo que ele não precisa de argumentos de entrada)
    comando_magalu = ["python", "processador_ETL_magalu.py"]
    executar_processo(comando_magalu)
    
    # --- Processar Balancetes Época ---
    # Lista dos arquivos de entrada da Época
    arquivos_epoca_input = [
        "caminho/para/seu/balancete_epoca_mes_anterior.xlsx",
        "caminho/para/seu/balancete_epoca_mes_atual.xlsx"
    ]
    
    for arquivo in arquivos_epoca_input:
        comando_epoca = ["python", "processador_ELT_epoca.py", "--input", arquivo]
        executar_processo(comando_epoca)
        
    logging.info("--- Fim da orquestração ---")