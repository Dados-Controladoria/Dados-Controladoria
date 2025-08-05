# --- 1. CONFIGURAÇÃO E IMPORTAÇÕES ---
import pandas as pd
import os 
from openpyxl import Workbook
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- CAMINHOS DE ENTRADA E SAÍDA (Ajuste se necessário) ---
CAMINHO_DE_ENTRADA = r"C:\Users\jea_goncalves\Desktop\balancetes"
CAMINHO_DE_SAIDA_XLSX = r"C:\Users\jea_goncalves\Desktop\balancetes\saidas\senior"
CAMINHO_DE_SAIDA_PKL = r"C:\Users\jea_goncalves\Desktop\balancetes\saidas\pkl"

# --- 2. FUNÇÕES AUXILIARES ---
def formatar_saldos(valor):
    """Limpa e converte uma string de saldo (com 'C' ou 'D') para um número float."""
    valor_str = str(valor).strip()
    valor_limpo = valor_str.replace('.', '').replace(',', '.')
    if valor_limpo.endswith('C'):
        return float('-' + valor_limpo[:-1])
    elif valor_limpo.endswith('D'):
        return float(valor_limpo[:-1])
    return float(valor_limpo)

# ==============================================================================
# --- 3. LÓGICA DE AUTOMAÇÃO ---
# ==============================================================================

# Calcula dinamicamente os nomes dos arquivos de entrada esperados
hoje = datetime.now()
nome_arquivo_mes_atual = hoje.strftime('%m-%y') + ".xlsx"
nome_arquivo_mes_anterior = (hoje - relativedelta(months=1)).strftime('%m-%y') + ".xlsx"

# Cria uma lista dos arquivos que o script deve procurar e processar
arquivos_para_processar = [nome_arquivo_mes_anterior, nome_arquivo_mes_atual]

print("--- Iniciando processamento automatizado dos balancetes da Época ---")
print(f"Arquivos a serem procurados: {arquivos_para_processar}")

# Loop principal que processa cada arquivo da lista
for nome_arquivo in arquivos_para_processar:
    
    print(f"\n--- Procurando por: {nome_arquivo} ---")
    
    caminho_completo_entrada = os.path.join(CAMINHO_DE_ENTRADA, nome_arquivo)
    
    # Verifica se o arquivo de entrada realmente existe antes de continuar
    if not os.path.exists(caminho_completo_entrada):
        print(f"AVISO: Arquivo '{nome_arquivo}' não encontrado na pasta de entrada. Pulando.")
        continue # Pula para o próximo arquivo da lista

    # --- Lógica de ETL (seu código original, agora dentro do loop) ---
    print(f"Arquivo '{nome_arquivo}' encontrado. Iniciando processamento...")
    df = pd.read_excel(caminho_completo_entrada)
    
    # 4. LIMPEZA E TRANSFORMAÇÃO
    df_processado = (
        df.drop(columns=['Balancete Mensal', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 4', 'Unnamed: 6', 'Unnamed: 8', 'Unnamed: 11', 'Unnamed: 13'], errors='ignore')
        .dropna()
        .pipe(lambda df_: df_[~df_.isin(["Saldo Anterior"]).any(axis=1)])
        .rename(columns={'Unnamed: 0': 'Classificação', 'Unnamed: 3': 'Nome', 'Unnamed: 7': 'Saldo Anterior', 'Unnamed: 9': 'Débito', 'Unnamed: 10': 'Crédito', 'Unnamed: 12': 'Saldo Atual'})
    )
    df_processado['Classificação'] = df_processado['Classificação'].astype(str).str.replace('.', '', regex=False)
    df_processado = df_processado[df_processado['Classificação'].str.len() >= 10]

    # 5. FORMATAÇÃO E CONVERSÃO DE TIPOS
    df_processado['Saldo Anterior'] = df_processado['Saldo Anterior'].apply(formatar_saldos)
    df_processado['Saldo Atual'] = df_processado['Saldo Atual'].apply(formatar_saldos)
    df_processado['Débito'] = df_processado['Débito'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
    df_processado['Crédito'] = df_processado['Crédito'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
    print("Dados processados e formatados.")

    # ==============================================================================
    # 6. SALVANDO OS ARQUIVOS FINAIS (COM O NOME CORRETO)
    # ==============================================================================
    
    # AJUSTE: O identificador agora é o próprio nome do mês, sem a extensão .xlsx
    identificador_mes = nome_arquivo.replace('.xlsx', '')
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_base_saida = f"bal_epoca_{identificador_mes}_{timestamp}"
    
    # Salva o arquivo Pickle
    caminho_saida_pkl = os.path.join(CAMINHO_DE_SAIDA_PKL, nome_base_saida + ".pkl")
    df_processado.to_pickle(caminho_saida_pkl)
    print(f"Arquivo Pickle salvo: {os.path.basename(caminho_saida_pkl)}")

    # Salva o arquivo Excel
    caminho_saida_xlsx = os.path.join(CAMINHO_DE_SAIDA_XLSX, nome_base_saida + ".xlsx")
    with pd.ExcelWriter(caminho_saida_xlsx, engine="openpyxl") as writer:
        df_processado.to_excel(writer, sheet_name="Balancete Formatado", index=False)
    print(f"Arquivo Excel salvo: {os.path.basename(caminho_saida_xlsx)}")

print("\n--- Processamento dos balancetes da Época concluído. ---")