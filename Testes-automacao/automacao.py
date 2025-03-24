import subprocess
import logging
import sys

# Configuração do log
logging.basicConfig(
    filename="processo.log", 
    level=logging.INFO, 
    encoding="utf-8", 
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S")

logging.info("O script está sendo executado...")

try:
    
    # Executa o script e captura a execução do arquivo e se deu erro ou não. Caso der erro, vai retornar o erro que deu.
    resultado = subprocess.run(["python", "main.py"], capture_output=True, text=True, universal_newlines=True)

    # Captura erros (stderr)
    if resultado.stderr.strip():
        logging.error(f"Erro do script:\n{resultado.stderr}")
    else: 
        logging.info("O script 'main.py' foi executado com sucesso, sem erros.")
except KeyboardInterrupt:
    logging.warning("Execução interrompida pelo usuário.")
    sys.exit(1)
except Exception as e:
    logging.error(f"Execução encerrada devido a erro inesperado: {e}")
    sys.exit(1)

finally:
    logging.info("Execução encerrada.")




 # logging.info(resultado.stdout) - usar esse módulo do logging pra retornar a saída do arquivo assim que a formatação dos dados for realizada.

