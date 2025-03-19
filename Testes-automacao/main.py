import time
import subprocess
import os 
import logging
import subprocess
import logging



def cronometro ():
    segundos = 0
    minutos = 0
    for i in range(120):
        print(f"{minutos}:{segundos}")  
        time.sleep(1)
        segundos+= 1
        if  segundos == 60:
            segundos = 0
            minutos +=1

cronometro()

            

