import pandas as pd
import os
import openpyxl
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles.numbers import NumberFormat
from sqlalchemy import create_engine, text
import oracledb

DB_USER = "READER"
DB_PASSWORD = "READER"
TNS_NAME = "MLPEBS"

config_dir_path = r"C:\oracle\product\11.2.0\client_1\Network\Admin"

try:
    if os.path.exists(config_dir_path):
        oracledb.init_oracle_client(config_dir=config_dir_path)
        print(f"Oracle Cliente inicializado via código coódigo. Usando config_dir: {config_dir_path}")
    else:
        print(f"AVISO: Deu ruim, o diretório de configuração não existe")
except Exception as e_init:
    print(f"Erro ao tentar inicializar Oracle Client com config_dir (pode já estar inicializado): {e_init}")

db_connection_str = f'oracle+oracledb://{DB_USER}:{DB_PASSWORD}@{TNS_NAME}'

print(f"String de conexão gerada: {db_connection_str}")

try:
   
    engine = create_engine(db_connection_str)

  
    sql_query = text("""
    select  gl.name                                                                                         AS livro,
        MAX(gb.last_update_date)                                                                        AS ULTIMA_ALTERCAO_GL,
        TO_DATE(gb.period_name, 'MM-YY')                                                                AS data_efetiva,
        gb.period_name                                                                                  AS periodo,
        gcc.segment1                                                                                    AS empresa,
        gcc.segment3                                                                                    AS conta,
        ffvv.description                                                                                AS descricao_conta,
        SUM(gb.begin_balance_dr - gb.begin_balance_cr)                                                  AS saldo_inicial,
        SUM(gb.period_net_dr)                                                                           AS debito,
        SUM(gb.period_net_cr)                                                                           AS credito,
        SUM(gb.begin_balance_dr - gb.begin_balance_cr) + sum(gb.period_net_dr) - sum(gb.period_net_cr)  AS saldo_final,
        SUM (gb.period_net_dr - gb.period_net_cr)                                                       AS mov_mes
        
   from apps.gl_balances                  gb,
        apps.gl_code_combinations         gcc,
        apps.gl_ledgers                   gl,
        apps.fnd_flex_values_vl           ffvv
        
  where gb.code_combination_id            = gcc.code_combination_id
    and gb.ledger_id                      = gl.ledger_id
    and gcc.segment3                      = ffvv.flex_value
    and ffvv.flex_value_set_id            IN (1014008,1016808)
    and gb.ledger_id                      IN (2022,2042)
    and gb.actual_flag                    IN ('A')
    and gb.period_name                    IN ('03-25', '04-25')
    and gcc.segment3                      NOT LIKE '%ORC%'
    and gcc.segment1                      IN ('001','004','007','008','009','010','011', '012')
    
  group by gl.name,gb.period_name,gcc.segment1,gcc.segment3,ffvv.description
    """)

    print("Tentando conectar e executar a query...")

   
    df = pd.read_sql_query(sql_query, engine)

    print("\nDados carregados com sucesso!")

except Exception as e:
    print(f"\nOcorreu um erro: {e}") 

finally:
    if 'engine' in locals() and engine:
       engine.dispose()
       print("\nRecursos do Engine liberados.")

df['ultima_altercao_gl'] = df['ultima_altercao_gl'].dt.strftime('%d/%m/%Y %H:%M:%S')

df['data_efetiva'] = df['data_efetiva'].dt.strftime('%d/%m/%Y')

df.to_excel(r"C:\Users\jea_goncalves\Desktop\bases_balancete\saidas\balancete_magalu_formatado.xlsx", index=False, engine="openpyxl")

arquivoSalvo = r"C:\Users\jea_goncalves\Desktop\bases_balancete\saidas\balancete_magalu_formatado.xlsx"

colunasParaFormatar = [8,9,10,11,12]

with pd.ExcelWriter(arquivoSalvo, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="Sheet1",index=False)

    workbook = writer.book
    worksheet = workbook["Sheet1"]

    for colunasDesejadas in colunasParaFormatar:
        for cell in worksheet.iter_cols(min_col=colunasDesejadas, max_col=colunasDesejadas, min_row=2):
            for c in cell:
                c.number_format = '0.00'
                
print("Formatação concluída")        