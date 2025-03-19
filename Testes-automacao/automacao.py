
import subprocess
import logging


logging.basicConfig(filename="processo.log", level=logging.INFO)

resultado = subprocess.run(["python", "main.py"], capture_output=True, text=True)
logging.info(resultado.stdout)




