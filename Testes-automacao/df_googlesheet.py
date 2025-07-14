import gspread
from gspread import oauth
import pandas as pd
from dotenv import load_dotenv
import os
import numpy as np


dfBalancete_magalu = pd.read_pickle(r"C:\Users\jea_goncalves\Desktop\bases_balancete\saidas\balancete-processado_Mlgu.pkl")
dfBalancete_epoca = pd.read_pickle(r"C:\Users\jea_goncalves\Desktop\bases_balancete\saidas\balancete-processado_Epca.pkl")

dfBalancete_epoca.head()
dfBalancete_magalu.head()


load_dotenv()
CAMINHO_CREDENCIAL_JSON = os.getenv('GOOGLE_CREDENTIALS_SERVICE_ACCOUNT')
ID_PLANILHA = os.getenv('GOOGLE_SHEET_ID')
NOME_ABA  = os.getenv('WORKSHEET_NAME')
TOKEN_USER = os.getenv('AUTHORIZED_USER_FILE_PATH')


gc = gspread.service_account(
    filename=CAMINHO_CREDENCIAL_JSON)

ws = gc.open_by_key(ID_PLANILHA)
aba = ws.worksheet(NOME_ABA)
data = aba.get_all_records()
df = pd.DataFrame(data)

df.head(10000)

from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_target_month_strings():
    today = datetime(2025,6,1)
    # Para consistência com o exemplo "05-25", vamos fixar as datas.
    # Remova ou ajuste estas linhas para usar datas dinâmicas reais.
    # today = datetime(2025, 6, 1) # Exemplo: Mês atual é Junho de 2025

    current_month_year_obj = today
    previous_month_year_obj = today - relativedelta(months=1)

    # Formato MM-AA (ex: "06-25", "05-25")
    current_month_str = current_month_year_obj.strftime("%m-%y")
    prev_month_str = previous_month_year_obj.strftime("%m-%y")
    return current_month_str, prev_month_str

CURRENT_MONTH_STR, PREV_MONTH_STR = get_target_month_strings()
print(f"Mês Anterior Alvo: {PREV_MONTH_STR}")
print(f"Mês Atual Alvo: {CURRENT_MONTH_STR}")


COL_PERIODO = "PERIODO"
COL_EMPRESA = "EMPRESA"
COL_A_FORMULA = "CONCATENAÇÃO" # Coluna A da planilha
COL_B_BAL1_START = "LIVRO" # Exemplo da primeira coluna de dados do Balancete 1
# ... adicione outros nomes de coluna do df_sheets se necessário para referência
COL_G_BAL2_START_CONTAS = "CONTA" # Exemplo da coluna "Contas" do Balancete 2 (em G)
COL_M_FORMULA = "MOV_MES" # Coluna M da planilha

# Placeholder para as fórmulas
FORMULA_A_PLACEHOLDER = "=CONCATENAR(F_val,G_val)" # Exemplo, ajuste para sua fórmula real
FORMULA_M_PLACEHOLDER = "=J_val-K_val" # Exemplo, ajuste para sua fórmula real

# Colunas que são copiadas de cima para baixo no Balancete 2
BAL2_COLS_COPIED_DOWN = ['LIVRO', 'DATA_EFETIVA', 'PERIODO', 'EMPRESA']
# Assumindo que essas são as colunas B, D, E, F respectivamente, como mencionado.
# Seus nomes reais no df_sheets podem ser diferentes.
# No exemplo de df_sheets, vou usar nomes genéricos como 'SheetColB', 'SheetColD', etc.
# E os balancetes ETL terão colunas com os dados de valor.

# --- DEFINA AQUI A ORDEM DAS COLUNAS DO SEU `df_sheets` ---
# Isto é crucial para criar novas linhas corretamente.
# Substitua pela lista real de colunas da sua planilha.
ORDERED_SHEET_COLUMNS = [
    COL_A_FORMULA, 'LIVRO', 'ULTIMA_ALTERCAO_GL', 'DATA_EFETIVA', 'PERIODO', 'EMPRESA',
    COL_G_BAL2_START_CONTAS, 'DESCRICAO_CONTA', 'SALDO_INICIAL', 'DEBITO', 'CREDITO', 'SALDO_FINAL',
    COL_M_FORMULA, COL_PERIODO, COL_EMPRESA
]


df_sheets = pd.DataFrame(data, columns=ORDERED_SHEET_COLUMNS)
# Adicionando uma linha "header" B1 simbólica para o index 0, para que B2 seja index 1
# A linha B2 da planilha corresponde ao df_sheets.iloc[1] (se não houver header no DataFrame)
# Ou df_sheets.loc[2] se o índice começar em 1 e tiver uma linha de header no índice 1.
# Para simplificar, vamos assumir que a "linha B2" se refere ao df_sheets.iloc[1] para o primeiro bloco de dados.

print("df_sheets Inicial:")
df_sheets.head(15000)



# Colunas de VALOR que serão copiadas do df_bal1 para o df_sheets (a partir da SheetColB)
# Estas devem corresponder às colunas em df_sheets de COL_B_BAL1_START em diante.
# Ex: 'ValorConta1', 'ValorConta2' irão para 'SheetColB', 'SheetColC'
BAL1_ETL_VALUE_COLS = ['ValorConta1_B1', 'ValorConta2_B1', 'ValorConta3_B1', 'ValorConta4_B1', 'ValorConta5_B1', 'ValorConta6_B1', 'ValorConta7_B1', 
                       'ValorConta8_B1', 'ValorConta9_B1', 'ValorConta10_B1', 'ValorConta11_B1']
# O número de colunas em BAL1_ETL_VALUE_COLS deve corresponder ao número de colunas de dados do Bal1 no df_sheets





df_bal1_data = {
    COL_PERIODO: [PREV_MONTH_STR, PREV_MONTH_STR, PREV_MONTH_STR, CURRENT_MONTH_STR],
    COL_EMPRESA: ['1', '1', '1', '1'], # Balancete 1 sempre tem empresa <> '2' (ex: '1')
    'ValorConta1_B1': [10, 20, 25, 30],
    'ValorConta2_B1': [11, 21, 26, 31],
    'ValorConta3_B1': [12, 22, 27, 32],
    'ValorConta4_B1': [13, 23, 28, 33],
    'ValorConta5_B1': [14, 24, 29, 34],
    'ValorConta6_B1': [15, 25, 30, 35],
    'ValorConta7_B1': [16, 26, 31, 36],
    'ValorConta8_B1': [17, 27, 32, 37],
    'ValorConta9_B1': [18, 28, 33, 38],
    'ValorConta10_B1': [19, 29, 34, 39],
    'ValorConta11_B1': [20, 30, 35, 40]
}




df_bal1 = pd.DataFrame(df_bal1_data)
print("\ndf_bal1 (ETL):")
print(df_bal1)




# Exemplo: df_bal2_prev_month (Balancete 2 - Mês Anterior)
# Colunas de VALOR que serão copiadas do df_bal2 para o df_sheets (a partir de COL_G_BAL2_START_CONTAS)
BAL2_ETL_VALUE_COLS = ['Contas_B2', 'Val2_B2', 'Val3_B2', 'Val4_B2', 'Val5_B2', 'Val6_B2']
# O número de colunas em BAL2_ETL_VALUE_COLS deve corresponder ao número de colunas de dados do Bal2 no df_sheets (G em diante)



df_bal2_prev_data = {
    COL_PERIODO: [PREV_MONTH_STR],
    COL_EMPRESA: ['2'], # Balancete 2 sempre tem empresa '2'
    'Contas_B2': [600], # Vai para COL_G_BAL2_START_CONTAS
    'Val2_B2': [60],    # Vai para SheetColH
    'Val3_B2': [61],
    'Val4_B2': [62],
    'Val5_B2': [63],
    'Val6_B2': [64]
}


df_bal2_prev_month = pd.DataFrame(df_bal2_prev_data)
print("\ndf_bal2_prev_month (ETL):")
print(df_bal2_prev_month)


df_bal2_curr_data = {
    COL_PERIODO: [CURRENT_MONTH_STR, CURRENT_MONTH_STR, CURRENT_MONTH_STR],
    COL_EMPRESA: ['2', '2', '2'],
    'Contas_B2': [700, 710, 720],
    'Val2_B2': [70, 71, 72],
    'Val3_B2': [73, 74, 75],
    'Val4_B2': [76, 77, 78],
    'Val5_B2': [79, 80, 81],
    'Val6_B2': [82, 83, 84]
}


df_bal2_curr_month = pd.DataFrame(df_bal2_curr_data)
print("\ndf_bal2_curr_month (ETL):")
print(df_bal2_curr_month)


BAL2_SHEET_COLS_COPIED_DOWN = ['SheetColB', 'SheetColD', 'SheetColE', 'SheetColF']


def update_sheet_section(
    df_main_sheet,
    etl_data_for_period,
    target_period,
    is_bal_type_1, # True para Balancete 1, False para Balancete 2
    ordered_sheet_cols, # Lista com todos os nomes de coluna do df_sheets na ordem correta
    bal1_etl_val_cols, # Lista de colunas do ETL Bal1 a serem coladas
    bal1_sheet_start_col_name, # Nome da coluna no df_sheets onde começam os dados do Bal1 (ex: 'SheetColB')
    bal2_etl_val_cols, # Lista de colunas do ETL Bal2 a serem coladas
    bal2_sheet_start_col_name, # Nome da coluna no df_sheets onde começam os dados do Bal2 (ex: 'SheetColG_Contas')
    bal2_sheet_cols_to_copy_down # Lista de colunas no df_sheets a serem copiadas de cima para baixo para Bal2
):
    print(f"\n--- Processando: {'Balancete 1' if is_bal_type_1 else 'Balancete 2'} para o período {target_period} ---")
     # Garantir que o DataFrame principal tenha um índice resetado se não for RangeIndex
    # Isso pode ser necessário se houver exclusões e concatenações frequentes.
    # No entanto, para inserções e deleções usando iloc/loc e concat, o pandas geralmente lida bem,
    # mas é bom ter cuidado se os índices ficarem não sequenciais.
    # df_main_sheet = df_main_sheet.reset_index(drop=True) # Descomente se necessário

    # 1. Identificar o bloco existente no df_sheets
    if is_bal_type_1:
        sheet_block_filter = (df_main_sheet[COL_PERIODO] == target_period) & (df_main_sheet[COL_EMPRESA] != '2')
    else: # Balancete 2
        sheet_block_filter = (df_main_sheet[COL_PERIODO] == target_period) & (df_main_sheet[COL_EMPRESA] == '2')

    current_block_indices = df_main_sheet[sheet_block_filter].index
    num_sheet_rows = len(current_block_indices)
    num_etl_rows = len(etl_data_for_period)

    print(f"Linhas existentes no df_sheets para este bloco: {num_sheet_rows}")
    print(f"Linhas no ETL para este bloco: {num_etl_rows}")

     # Determinar o ponto de partida para colar os dados do ETL
    if is_bal_type_1:
        sheet_cols_to_paste_into = ordered_sheet_cols[ordered_sheet_cols.index(bal1_sheet_start_col_name) : ordered_sheet_cols.index(bal1_sheet_start_col_name) + len(bal1_etl_val_cols)]
        etl_value_cols = bal1_etl_val_cols
    else: # Balancete 2
        sheet_cols_to_paste_into = ordered_sheet_cols[ordered_sheet_cols.index(bal2_sheet_start_col_name) : ordered_sheet_cols.index(bal2_sheet_start_col_name) + len(bal2_etl_val_cols)]
        etl_value_cols = bal2_etl_val_cols


    if num_etl_rows == 0 and num_sheet_rows > 0: # Apagar todas as linhas do bloco
        print(f"ETL vazio, apagando {num_sheet_rows} linhas do bloco no df_sheets.")
        df_main_sheet = df_main_sheet.drop(current_block_indices).reset_index(drop=True)
        return df_main_sheet

    if num_etl_rows == 0 and num_sheet_rows == 0: # Nada a fazer
        print("Nada no ETL e nada no df_sheets para este bloco. Nenhuma ação.")
        return df_main_sheet
    # 2. Ajustar o número de linhas no df_sheets
    temp_df_main_sheet = df_main_sheet.copy() # Trabalhar em uma cópia para concatenação

    if num_sheet_rows < num_etl_rows: # Adicionar linhas
        rows_to_add_count = num_etl_rows - num_sheet_rows
        print(f"Adicionando {rows_to_add_count} linhas ao df_sheets.")
        new_rows_list = []
        for _ in range(rows_to_add_count):
            new_row_data = {col: np.nan for col in ordered_sheet_cols} # Começa com NaNs
            new_row_data[COL_PERIODO] = target_period
            new_row_data[COL_EMPRESA] = '1' if is_bal_type_1 else '2'
            new_row_data[COL_A_FORMULA] = FORMULA_A_PLACEHOLDER
            if not is_bal_type_1:
                new_row_data[COL_M_FORMULA] = FORMULA_M_PLACEHOLDER
            new_rows_list.append(new_row_data)
        new_rows_df = pd.DataFrame(new_rows_list, columns=ordered_sheet_cols)

        if not current_block_indices.empty: # Se o bloco existe, adiciona após ele
            insert_point_idx = current_block_indices.max() + 1
            temp_df_main_sheet = pd.concat([
                temp_df_main_sheet.iloc[:insert_point_idx],
                new_rows_df,
                temp_df_main_sheet.iloc[insert_point_idx:]
            ]).reset_index(drop=True)
        else: # Bloco não existe, precisa encontrar onde inserir (lógica complexa de ordenação global)
              # Para este exemplo, vamos assumir que se o bloco não existe, ele é anexado ao final do período
              # ou em uma ordem específica (B1 antes de B2 do mesmo período, Período Anterior antes de Atual).
              # Esta parte da lógica de inserção de um *novo bloco inteiro* em um local específico
              # precisaria de mais regras sobre a ordem global dos blocos.
              # Por ora, se um bloco estiver faltando, esta função vai anexá-lo de forma simples,
              # o que pode não ser a ordem visual correta na planilha.
              # A melhor abordagem é que `df_sheets` já tenha pelo menos uma linha dummy para cada bloco.
            print(f"AVISO: Bloco para {'Bal1' if is_bal_type_1 else 'Bal2'} {target_period} não encontrado. Anexando novas linhas.")
            # Tenta encontrar o final do período anterior ou do balancete anterior no mesmo período
            # Esta é uma simplificação e pode precisar de ajuste fino.
            relevant_period_data = temp_df_main_sheet[temp_df_main_sheet[COL_PERIODO] == target_period]
            if not relevant_period_data.empty:
                insert_point_idx = relevant_period_data.index.max() + 1
                temp_df_main_sheet = pd.concat([
                    temp_df_main_sheet.iloc[:insert_point_idx],
                    new_rows_df,
                    temp_df_main_sheet.iloc[insert_point_idx:]
                ]).reset_index(drop=True)
            else: # Se nem o período existe, anexa ao final
                 temp_df_main_sheet = pd.concat([temp_df_main_sheet, new_rows_df]).reset_index(drop=True)


    elif num_sheet_rows > num_etl_rows: # Deletar linhas
        rows_to_delete_count = num_sheet_rows - num_etl_rows
        print(f"Deletando {rows_to_delete_count} linhas do df_sheets.")
        # Deleta as últimas linhas excedentes do bloco
        indices_to_drop = current_block_indices[-rows_to_delete_count:]
        temp_df_main_sheet = temp_df_main_sheet.drop(indices_to_drop).reset_index(drop=True)

    # Re-identificar o bloco após ajustes de tamanho
    if is_bal_type_1:
        final_block_filter = (temp_df_main_sheet[COL_PERIODO] == target_period) & (temp_df_main_sheet[COL_EMPRESA] != '2')
    else:
        final_block_filter = (temp_df_main_sheet[COL_PERIODO] == target_period) & (temp_df_main_sheet[COL_EMPRESA] == '2')
    final_block_indices = temp_df_main_sheet[final_block_filter].index

    # Garantir que temos o número certo de índices para os dados do ETL
    if len(final_block_indices) != num_etl_rows:
        print(f"ERRO: Discrepância no tamanho do bloco final. Esperado: {num_etl_rows}, Obtido: {len(final_block_indices)}")
        # Isso pode acontecer se a lógica de inserção/deleção não funcionar como esperado
        # ou se o bloco não existir e a inserção anexa simples não for adequada.
        # Por segurança, retorna o DataFrame sem colar os dados se houver essa discrepância.
        return df_main_sheet # Retorna o original se algo deu muito errado

    # 3. Colar os dados do ETL
    print(f"Colando dados do ETL no bloco final (índices: {final_block_indices.tolist()}).")
    if not etl_data_for_period.empty and not final_block_indices.empty:
        for i, etl_col_name in enumerate(etl_value_cols):
            sheet_col_name = sheet_cols_to_paste_into[i]
            # Usar .values para evitar problemas de alinhamento de índice ao atribuir
            temp_df_main_sheet.loc[final_block_indices, sheet_col_name] = etl_data_for_period[etl_col_name].values

        # Preencher fórmulas novamente para todas as linhas do bloco final
        temp_df_main_sheet.loc[final_block_indices, COL_A_FORMULA] = FORMULA_A_PLACEHOLDER
        if not is_bal_type_1: # Balancete 2
            temp_df_main_sheet.loc[final_block_indices, COL_M_FORMULA] = FORMULA_M_PLACEHOLDER
            # Copiar valores das colunas B, D, E, F de cima para baixo DENTRO do bloco Bal2
            if not final_block_indices.empty and bal2_sheet_cols_to_copy_down:
                first_row_idx_in_block = final_block_indices.min()
                for idx in final_block_indices:
                    if idx == first_row_idx_in_block:
                        # Para a primeira linha do bloco Bal2, precisamos de uma fonte para B,D,E,F.
                        # Se este bloco foi recém-criado ou a linha acima não é Bal2 do mesmo período,
                        # esta lógica pode precisar de ajuste.
                        # Assumindo que se há uma linha acima (idx-1) ela pode ser uma fonte válida
                        # ou os valores já estão corretos por inserção anterior.
                        if idx > 0: # Se não for a primeira linha absoluta do DataFrame
                             # Tenta copiar da linha imediatamente acima, se essa linha não fizer parte do bloco atual
                             # e tiver os mesmos valores de período (ou se for para copiar de qualquer forma).
                             # Esta é uma simplificação. A fonte para a primeira linha de um bloco Bal2
                             # se as colunas B,D,E,F não estiverem no ETL é um ponto crítico.
                             # Se o bloco já existia, a primeira linha do bloco mantém seus valores.
                             # Se foi uma nova linha adicionada no início do bloco Bal2,
                             # ela deveria idealmente herdar da última linha do bloco Bal1 do mesmo período,
                             # ou ter valores padrão.
                             # A lógica atual de inserção de new_rows_df já preenche com NaN.
                             # Vamos assumir que a primeira linha do bloco já tem, ou deveria ter, os valores corretos de B,D,E,F
                             # e a cópia é para as linhas *subsequentes dentro do bloco*.
                             pass # A primeira linha do bloco mantém ou obtém seus valores de B,D,E,F de outra forma.
                    else: # Para as demais linhas do bloco, copia da linha de cima DENTRO do bloco.
                        temp_df_main_sheet.loc[idx, bal2_sheet_cols_to_copy_down] = temp_df_main_sheet.loc[idx-1, bal2_sheet_cols_to_copy_down].values
    else:
        print("Dados do ETL vazios ou bloco final no df_sheets não encontrado/vazio. Nenhuma colagem de dados.")

    return temp_df_main_sheet


# --- PASSO 1: Balancete 1 - Mês Anterior ---
etl_bal1_prev = df_bal1[df_bal1[COL_PERIODO] == PREV_MONTH_STR]
df_sheets = update_sheet_section(
    df_sheets,
    etl_bal1_prev,
    PREV_MONTH_STR,
    is_bal_type_1=True,
    ordered_sheet_cols=ORDERED_SHEET_COLUMNS,
    bal1_etl_val_cols=BAL1_ETL_VALUE_COLS,
    bal1_sheet_start_col_name='SheetColB', # Primeira coluna de dados do Bal1 no df_sheets
    bal2_etl_val_cols=None, # Não aplicável
    bal2_sheet_start_col_name=None, # Não aplicável
    bal2_sheet_cols_to_copy_down=None # Não aplicável
)
print("\nDF_SHEETS após Balancete 1 - Mês Anterior:")
print(df_sheets)

# --- PASSO 2: Balancete 2 - Mês Anterior ---
etl_bal2_prev = df_bal2_prev_month[df_bal2_prev_month[COL_PERIODO] == PREV_MONTH_STR] # Já filtrado, mas para garantir
df_sheets = update_sheet_section(
    df_sheets,
    etl_bal2_prev,
    PREV_MONTH_STR,
    is_bal_type_1=False,
    ordered_sheet_cols=ORDERED_SHEET_COLUMNS,
    bal1_etl_val_cols=None, # Não aplicável
    bal1_sheet_start_col_name=None, # Não aplicável
    bal2_etl_val_cols=BAL2_ETL_VALUE_COLS,
    bal2_sheet_start_col_name=COL_G_BAL2_START_CONTAS, # Coluna 'Contas' do Bal2 no df_sheets
    bal2_sheet_cols_to_copy_down=BAL2_SHEET_COLS_COPIED_DOWN
)
print("\nDF_SHEETS após Balancete 2 - Mês Anterior:")
print(df_sheets)


# --- PASSO 3: Balancete 1 - Mês Atual ---
etl_bal1_curr = df_bal1[df_bal1[COL_PERIODO] == CURRENT_MONTH_STR]
df_sheets = update_sheet_section(
    df_sheets,
    etl_bal1_curr,
    CURRENT_MONTH_STR,
    is_bal_type_1=True,
    ordered_sheet_cols=ORDERED_SHEET_COLUMNS,
    bal1_etl_val_cols=BAL1_ETL_VALUE_COLS,
    bal1_sheet_start_col_name='SheetColB',
    bal2_etl_val_cols=None,
    bal2_sheet_start_col_name=None,
    bal2_sheet_cols_to_copy_down=None
)
print("\nDF_SHEETS após Balancete 1 - Mês Atual:")
print(df_sheets)

# --- PASSO 4: Balancete 2 - Mês Atual ---
etl_bal2_curr = df_bal2_curr_month[df_bal2_curr_month[COL_PERIODO] == CURRENT_MONTH_STR] # Já filtrado, mas para garantir
df_sheets = update_sheet_section(
    df_sheets,
    etl_bal2_curr,
    CURRENT_MONTH_STR,
    is_bal_type_1=False,
    ordered_sheet_cols=ORDERED_SHEET_COLUMNS,
    bal1_etl_val_cols=None,
    bal1_sheet_start_col_name=None,
    bal2_etl_val_cols=BAL2_ETL_VALUE_COLS,
    bal2_sheet_start_col_name=COL_G_BAL2_START_CONTAS,
    bal2_sheet_cols_to_copy_down=BAL2_SHEET_COLS_COPIED_DOWN
)
print("\nDF_SHEETS após Balancete 2 - Mês Atual (FINAL):")
print(df_sheets)

