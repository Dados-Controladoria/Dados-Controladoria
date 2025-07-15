# --- 1. CONFIGURAÇÃO E IMPORTAÇÕES ---
import pandas as pd
import os 
import glob
from openpyxl import Workbook
from datetime import datetime
from dateutil.relativedelta import relativedelta


# ==============================================================================
# --- CONFIGURAÇÃO MANUAL PROVISÓRIA ---
#
# Passo 1: Defina os nomes dos arquivos que você baixou.
MES_ATUAL_NOME_ARQUIVO = "07-25.xlsx"
MES_ANTERIOR_NOME_ARQUIVO = "06-25.xlsx"

# Passo 2: Escolha qual arquivo você quer processar NESTA execução.
#          - Para processar o mês ANTERIOR, use: MES_ANTERIOR_NOME_ARQUIVO
#          - Para processar o mês ATUAL, use: MES_ATUAL_NOME_ARQUIVO
#
ARQUIVO_A_PROCESSAR = MES_ATUAL_NOME_ARQUIVO
#
# ==============================================================================


# --- CAMINHOS DE ENTRADA E SAÍDA ---
CAMINHO_DE_ENTRADA = r"C:\Users\jea_goncalves\Desktop\balancetes"
CAMINHO_DE_SAIDA = r"C:\Users\jea_goncalves\Desktop\balancetes\saidas\senior"

# --- 2. FUNÇÕES AUXILIARES ---
def formatar_saldos(valor):
    valor_str = str(valor).strip()
    valor_limpo = valor_str.replace('.', '').replace(',', '.')
    if valor_limpo.endswith('C'):
        return float('-' + valor_limpo[:-1])
    elif valor_limpo.endswith('D'):
        return float(valor_limpo[:-1])
    return float(valor_limpo)

# --- 3. CARREGAMENTO DOS DADOS ---
caminho_completo_entrada = os.path.join(CAMINHO_DE_ENTRADA, ARQUIVO_A_PROCESSAR)
print(f"--- Processando arquivo: {ARQUIVO_A_PROCESSAR} ---")
df = pd.read_excel(caminho_completo_entrada)
print("Arquivo carregado com sucesso. Iniciando limpeza...")

# --- 4. LIMPEZA E TRANSFORMAÇÃO (Seu código original mantido) ---
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
df_processado['Classificação'] = df_processado['Classificação'].astype(str)
df_processado['Classificação'] = df_processado['Classificação'].str.replace('.', '', regex=False)
df_processado = df_processado[df_processado['Classificação'].str.len() >= 10]

# --- 5. FORMATAÇÃO E CONVERSÃO DE TIPOS (Seu código original mantido) ---
df_processado['Saldo Anterior'] = df_processado['Saldo Anterior'].apply(formatar_saldos)
df_processado['Saldo Atual'] = df_processado['Saldo Atual'].apply(formatar_saldos)
df_processado['Débito'] = df_processado['Débito'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
df_processado['Crédito'] = df_processado['Crédito'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
print("Dados processados e formatados.")

# --- 6. SALVANDO OS ARQUIVOS FINAIS (COM O NOME CORRETO) ---

def obter_strings_de_mes():
    """Calcula dinamicamente as strings para o mês atual e anterior no formato 'MM-YY'."""
    hoje = datetime.now()
    mes_atual = hoje.strftime("%m-%y")
    
    mes_anterior_data = hoje - relativedelta(months=1)
    mes_anterior = mes_anterior_data.strftime("%m-%y")
    
    return mes_atual, mes_anterior

mes_atual_str, mes_anterior_str = obter_strings_de_mes()

# Determina o identificador do mês com base na sua escolha no "Passo 2"
if ARQUIVO_A_PROCESSAR == MES_ATUAL_NOME_ARQUIVO:
    identificador_mes = mes_atual_str
elif ARQUIVO_A_PROCESSAR == MES_ANTERIOR_NOME_ARQUIVO:
    identificador_mes = mes_anterior_str


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
nome_base_saida = f"bal_epoca_{identificador_mes}_{timestamp}"

CAMINHO_DE_SAIDA_PKL = r"C:\Users\jea_goncalves\Desktop\balancetes\saidas\pkl"

# Caminho completo para o arquivo .pkl
caminho_saida_pkl = os.path.join(CAMINHO_DE_SAIDA_PKL, nome_base_saida + ".pkl")
# Caminho completo para o arquivo .xlsx
caminho_saida_xlsx = os.path.join(CAMINHO_DE_SAIDA, nome_base_saida + ".xlsx")

# Salva o arquivo .pkl (o mais importante para o seu notebook de integração)
df_processado.to_pickle(caminho_saida_pkl)
print(f"\nArquivo Pickle salvo em: {os.path.basename(caminho_saida_pkl)}")

# Salva o arquivo .xlsx formatado
with pd.ExcelWriter(caminho_saida_xlsx, engine="openpyxl") as writer:
    df_processado.to_excel(writer, sheet_name="Balancete Formatado", index=False)
    worksheet = writer.sheets["Balancete Formatado"]
    colunas_para_formatar = [3, 4, 5, 6] 
    formato_numerico = '#,##0.00'
    for indice_coluna in colunas_para_formatar:
        for cell in worksheet.iter_cols(min_col=indice_coluna, max_col=indice_coluna, min_row=2):
            for c in cell:
                c.number_format = formato_numerico
                
print(f"Arquivo Excel salvo em: {os.path.basename(caminho_saida_xlsx)}")
print("\nProcesso concluído com sucesso!")