# --- 1. CONFIGURAÇÃO E IMPORTAÇÕES ---
import pandas as pd
import os
from openpyxl import Workbook
from openpyxl.utils import get_column_letter # Import necessário para formatação
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- CAMINHOS DE ENTRADA E SAÍDA (Ajuste se necessário) ---
CAMINHO_DE_ENTRADA = r"C:\Users\jea_goncalves\Desktop\balancetes"
CAMINHO_DE_SAIDA_XLSX = r"C:\Users\jea_goncalves\Desktop\balancetes\saidas\senior"
CAMINHO_DE_SAIDA_PKL = r"C:\Users\jea_goncalves\Desktop\balancetes\saidas\pkl"

# --- 2. FUNÇÃO AUXILIAR CORRIGIDA ---
def parse_numero_brasileiro(valor):
    """
    Converte um valor (string ou número) para float de forma robusta.
    Lida com o formato brasileiro ('1.234,56') e com números já formatados com ponto decimal.
    Também trata os sufixos 'C' (Crédito/negativo).
    """
    # Se o valor já for um número (int, float), não há o que fazer.
    if isinstance(valor, (int, float)):
        return float(valor)

    # Converte para string para garantir que podemos manipular com segurança
    valor_str = str(valor).strip()

    # Se a string for vazia ou não representativa, retorna 0
    if not valor_str or valor_str.lower() == 'nan':
        return 0.0

    # Verifica se é um valor de crédito (negativo) e remove o 'C'
    is_creditor = valor_str.upper().endswith('C')
    if is_creditor:
        valor_str = valor_str[:-1].strip()
        
    # Verifica se é um valor de débito e remove o 'D' (não muda o sinal)
    if valor_str.upper().endswith('D'):
        valor_str = valor_str[:-1].strip()

    # LÓGICA DE LIMPEZA CORRIGIDA:
    # Se a string contém vírgula, tratamos como formato brasileiro "1.234,56"
    if ',' in valor_str:
        # Remove os pontos de milhar e troca a vírgula decimal por ponto
        valor_limpo = valor_str.replace('.', '').replace(',', '.')
    else:
        # Se não há vírgula, assumimos que já é um formato com ponto decimal (ex: "1234.56") ou inteiro.
        # Portanto, NÃO removemos o ponto, pois ele é o separador decimal.
        valor_limpo = valor_str
        
    try:
        numero = float(valor_limpo)
    except (ValueError, TypeError):
        # Se algo der errado na conversão, retorna 0 para evitar que o script pare
        return 0.0
    
    # Aplica o sinal negativo se for um valor credor
    return -numero if is_creditor else numero


# ==============================================================================
# --- 3. LÓGICA DE AUTOMAÇÃO ---
# ==============================================================================

# Cria os diretórios de saída se eles não existirem
os.makedirs(CAMINHO_DE_SAIDA_XLSX, exist_ok=True)
os.makedirs(CAMINHO_DE_SAIDA_PKL, exist_ok=True)

# Calcula dinamicamente os nomes dos arquivos de entrada esperados
hoje = datetime.now()
nome_arquivo_mes_atual = hoje.strftime('%m-%y') + ".xlsx"
nome_arquivo_mes_anterior = (hoje - relativedelta(months=1)).strftime('%m-%y') + ".xlsx"
arquivos_para_processar = [nome_arquivo_mes_anterior, nome_arquivo_mes_atual]

print("--- Iniciando processamento automatizado dos balancetes da Época ---")
print(f"Arquivos a serem procurados: {arquivos_para_processar}")

for nome_arquivo in arquivos_para_processar:
    print(f"\n--- Procurando por: {nome_arquivo} ---")
    caminho_completo_entrada = os.path.join(CAMINHO_DE_ENTRADA, nome_arquivo)
    
    if not os.path.exists(caminho_completo_entrada):
        print(f"AVISO: Arquivo '{nome_arquivo}' não encontrado na pasta de entrada. Pulando.")
        continue

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

    # 5. FORMATAÇÃO E CONVERSÃO DE TIPOS USANDO A NOVA FUNÇÃO
    print("Convertendo colunas numéricas com a lógica corrigida...")
    colunas_numericas = ['Saldo Anterior', 'Débito', 'Crédito', 'Saldo Atual']
    for col in colunas_numericas:
        df_processado[col] = df_processado[col].apply(parse_numero_brasileiro)
    print("Dados processados e formatados.")

    # ==============================================================================
    # 6. SALVANDO OS ARQUIVOS FINAIS
    # ==============================================================================
    
    identificador_mes = nome_arquivo.replace('.xlsx', '')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_base_saida = f"bal_epoca_{identificador_mes}_{timestamp}"
    
    # Salva o arquivo Pickle
    caminho_saida_pkl = os.path.join(CAMINHO_DE_SAIDA_PKL, nome_base_saida + ".pkl")
    df_processado.to_pickle(caminho_saida_pkl)
    print(f"Arquivo Pickle salvo: {os.path.basename(caminho_saida_pkl)}")

    # Salva o arquivo Excel e aplica a formatação de número com vírgula
    caminho_saida_xlsx = os.path.join(CAMINHO_DE_SAIDA_XLSX, nome_base_saida + ".xlsx")
    with pd.ExcelWriter(caminho_saida_xlsx, engine='openpyxl') as writer:
        df_processado.to_excel(writer, sheet_name='Balancete Formatado', index=False)
        
        workbook = writer.book
        worksheet = writer.sheets['Balancete Formatado']
        brazilian_number_format = '#,##0.00'

        for col_idx, col_name in enumerate(df_processado.columns, 1):
            if col_name in colunas_numericas:
                col_letter = get_column_letter(col_idx)
                for cell in worksheet[col_letter][1:]:
                    cell.number_format = brazilian_number_format

    print(f"Arquivo Excel salvo com formatação de vírgula: {os.path.basename(caminho_saida_xlsx)}")

print("\n--- Processamento dos balancetes da Época concluído. ---")