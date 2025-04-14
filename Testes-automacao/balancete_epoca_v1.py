import pandas as pd
import os 
import openpyxl
from IPython.display import FileLink
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles.numbers import NumberFormat

df = pd.read_excel(r"C:\Users\jea_goncalves\Desktop\bases_balancete\02-25teste3.xlsx")

df = df.drop(columns=['Balancete Mensal', 
                      'Unnamed: 4',
                      'Unnamed: 6',
                      'Unnamed: 11',
                      'Unnamed: 13',
                      'Unnamed: 1',
                      'Unnamed: 8',
                      'Unnamed: 2'])

apagarNulos = df.dropna()

apagarNulos = apagarNulos[~apagarNulos.isin(["Saldo Anterior"]).any(axis=1)]

apagarNulos['Unnamed: 0'] = apagarNulos['Unnamed: 0'].astype(str).replace(r'\.','', regex=True).replace(r'\s+','', regex=True)

apagarNulos = apagarNulos.rename(columns={'Unnamed: 0':'Classificação', 
                   'Unnamed: 3':'Nome',
                   'Unnamed: 7': 'Saldo Anterior', 
                   'Unnamed: 9': 'Débito', 
                   'Unnamed: 10': 'Crédito', 
                   'Unnamed: 12': 'Saldo Atual'})

apagarNulos = apagarNulos.astype({'Nome': str})

apagarNulos = apagarNulos.astype({'Saldo Anterior': str,'Saldo Atual': str})

def formatarSaldos(valor):
    valor = valor.replace('.','').replace(',','.')

    if valor.endswith('C'):
        valor = '-' + valor[:-1]
    else:
        valor.endswith('D')
        valor = valor[:-1]

    return float(valor)

alterarTipoDados = apagarNulos

alterarTipoDados['Saldo Anterior'] = alterarTipoDados['Saldo Anterior'].apply(formatarSaldos)
alterarTipoDados['Saldo Atual'] = alterarTipoDados['Saldo Atual'].apply(formatarSaldos)
alterarTipoDados['Débito'] = alterarTipoDados['Débito'].astype(str).replace('.','').replace(',','.')
alterarTipoDados['Crédito'] = alterarTipoDados['Crédito'].astype(str).replace('.','').replace(',','.')
alterarTipoDados = alterarTipoDados.astype({'Débito': float,'Crédito': float})
dadosFormatados = alterarTipoDados

print(dadosFormatados.dtypes)

dadosFormatados.to_excel(r"C:\Users\jea_goncalves\Desktop\bases_balancete\saidas\balancete_epoca_formatado.xlsx", index=False, engine="openpyxl")

arquivoSalvo = r"C:\Users\jea_goncalves\Desktop\bases_balancete\saidas\balancete_epoca_formatado.xlsx"

colunasParaFormatar = [3,4,5,6,7]

with pd.ExcelWriter(arquivoSalvo, engine="openpyxl") as writer:
    dadosFormatados.to_excel(writer, sheet_name="Sheet1",index=False)

    workbook = writer.book
    worksheet = workbook["Sheet1"]

    for colunasDesejadas in colunasParaFormatar:
        for cell in worksheet.iter_cols(min_col=colunasDesejadas, max_col=colunasDesejadas, min_row=2):
            for c in cell:
                c.number_format = '0.00'
                
print("Formatação concluída")        

