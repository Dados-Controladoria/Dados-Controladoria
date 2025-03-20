import time
import subprocess
import os 
import logging
import subprocess
import logging
import sys



def cronometro ():
    segundos = 0
    minutos = 0
    for i in range(60):
        sys.stdout.write(f"{minutos:00}:{segundos:00}\n")
        sys.stdout.flush()  
        time.sleep(1)
        segundos+= 1
        if  segundos == 60:
            segundos = 0
            minutos +=1

cronometro()

            

