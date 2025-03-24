import time
import sys

# Usar essas funções para retornar os valores de saída caso 
# for necessário em algum momento
# sys.stdout.write(f"{minutos:00}:{segundos:00}\n")
# sys.stdout.flush()

def cronometro ():
    segundos = 0
    minutos = 0
    for i in range(60):
        time.sleep(1)
        segundos+= 1
        if  segundos == 60:
            segundos = 0
            minutos +=1

cronometro()

            

