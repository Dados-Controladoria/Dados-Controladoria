# --- 1. CONFIGURAÇÃO E IMPORTAÇÕES ---
import pandas as pd
import os 
import glob
from openpyxl import Workbook
from datetime import datetime

# --- DEFINA SEUS CAMINHOS DE ENTRADA E SAÍDA AQUI ---
CAMINHO_DE_ENTRADA = r"C:\Users\jea_goncalves\Desktop\bases_balancete"
CAMINHO_DE_SAIDA = r"C:\Users\jea_goncalves\Desktop\bases_balancete\saidas"

# --- 2. FUNÇÕES AUXILIARES ---

def formatar_saldos(valor):
    """
    Limpa e converte uma string de saldo (com 'C' ou 'D') para um número float.
    'C' (Crédito) torna o número negativo. 'D' (Débito) torna positivo.
    """
    valor_str = str(valor).strip()
    valor_limpo = valor_str.replace('.', '').replace(',', '.')
    
    if valor_limpo.endswith('C'):
        return float('-' + valor_limpo[:-1])
    elif valor_limpo.endswith('D'):
        return float(valor_limpo[:-1])
    
    return float(valor_limpo)

# --- 3. CARREGAMENTO DOS DADOS ---
df = pd.read_excel(os.path.join(CAMINHO_DE_ENTRADA, "06-25.xlsx"))
print("Arquivo carregado com sucesso. Iniciando limpeza...")

# --- 4. LIMPEZA E TRANSFORMAÇÃO DOS DADOS ---
df_processado = (
    df
    .drop(columns=[
        'Balancete Mensal', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 4', 
        'Unnamed: 6', 'Unnamed: 8', 'Unnamed: 11', 'Unnamed: 13'
    ], errors='ignore')
    .dropna()
    .pipe(lambda df_: df_[~df_.isin(["Saldo Anterior"]).any(axis=1)])
    .rename(columns={
        'Unnamed: 0': 'Classificação',
        'Unnamed: 3': 'Nome',
        'Unnamed: 7': 'Saldo Anterior',
        'Unnamed: 9': 'Débito',
        'Unnamed: 10': 'Crédito',
        'Unnamed: 12': 'Saldo Atual'
    })
)
print("Colunas renomeadas e linhas de cabeçalho removidas.")

# ==============================================================================
# AJUSTE ADICIONADO CONFORME SOLICITADO
# ==============================================================================
# Primeiro, garante que 'Classificação' seja do tipo string para manipulação
df_processado['Classificação'] = df_processado['Classificação'].astype(str)

# 1. Remove os pontos da coluna 'Classificação'
df_processado['Classificação'] = df_processado['Classificação'].str.replace('.', '', regex=False)
print("Pontos da coluna 'Classificação' foram removidos.")

# 2. Remove linhas onde 'Classificação' (já sem os pontos) tem menos de 10 dígitos
linhas_antes_filtro = len(df_processado)
df_processado = df_processado[df_processado['Classificação'].str.len() >= 10]
linhas_depois_filtro = len(df_processado)
print(f"{linhas_antes_filtro - linhas_depois_filtro} linhas foram removidas por terem 'Classificação' com menos de 10 dígitos.")
# ==============================================================================

# --- 5. FORMATAÇÃO E CONVERSÃO DE TIPOS ---
df_processado['Saldo Anterior'] = df_processado['Saldo Anterior'].apply(formatar_saldos)
df_processado['Saldo Atual'] = df_processado['Saldo Atual'].apply(formatar_saldos)
df_processado['Débito'] = df_processado['Débito'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
df_processado['Crédito'] = df_processado['Crédito'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)

print("Tipos de dados convertidos para número.")
print("\nTipos de dados finais:")
print(df_processado.dtypes)

# --- 6. SALVANDO O ARQUIVO FINAL COM FORMATAÇÃO ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
nome_arquivo_final = f"balancete_epoca_processado_{timestamp}.xlsx"
caminho_completo_saida = os.path.join(CAMINHO_DE_SAIDA, nome_arquivo_final)

print(f"\nSalvando arquivo formatado em: {caminho_completo_saida}")

with pd.ExcelWriter(caminho_completo_saida, engine="openpyxl") as writer:
    df_processado.to_excel(writer, sheet_name="Balancete Formatado", index=False)
    worksheet = writer.sheets["Balancete Formatado"]

    colunas_para_formatar = [3, 4, 5, 6] 
    formato_numerico = '#,##0.00'

    for indice_coluna in colunas_para_formatar:
        for cell in worksheet.iter_cols(min_col=indice_coluna, max_col=indice_coluna, min_row=2):
            for c in cell:
                c.number_format = formato_numerico
                
print("Formatação concluída e arquivo salvo com sucesso!")