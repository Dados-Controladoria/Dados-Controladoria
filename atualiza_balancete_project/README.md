📊 Projeto de Automação de Balancetes
Este projeto automatiza o processo de coleta, tratamento e atualização de dados de balancetes (Magalu e Época) em uma planilha centralizadora no Google Sheets.

O fluxo é dividido em duas etapas independentes:

ETL: Scripts individuais processam os balancetes brutos, limpando e padronizando os dados antes de salvá-los em formatos otimizados (.pkl e .xlsx).

Integração: Um notebook principal carrega os dados processados e atualiza a planilha no Google Sheets de forma controlada e segura.

📋 Pré-requisitos
Acesso de leitura e escrita à planilha de destino no Google Sheets.

Conta de serviço do Google Cloud Platform (GCP) com a API do Google Sheets ativada.

Conda instalado para gerenciamento de pacotes.

⚙️ Configuração do Ambiente
Recomenda-se criar um ambiente Conda para isolar as dependências do projeto.

Crie o ambiente:

conda create --name automacao_balancetes python=3.9

Ative o ambiente:

conda activate atualiza_balancete_env 

Instale as bibliotecas:

conda install -c conda-forge pandas gspread oauth2client openpyxl python-dateutil jupyterlab

📁 Estrutura de Diretórios
O projeto utiliza a seguinte estrutura de pastas. Crie as que não existirem.

/seu_projeto/
│
├── 📁 entradas/
│   └── (Arquivos brutos de balancete .xls ou .csv)
│
├── 📁 saidas/
│   ├── 📁 pkl/
│   │   └── (Arquivos .pkl processados)
│   └── 📁 xlsx/
│       └── (Arquivos .xlsx formatados)
│
├── 📜 processador_ETL_magalu.ipynb
├── 📜 processador_ELT_epoca.ipynb
├── 📜 integracao_sheets.ipynb
└── 🔑 credentials.json  (Credenciais do GCP)

Arquivo de Credenciais
O arquivo credentials.json (obtido no GCP) é essencial para a conexão com o Google Sheets e deve estar na raiz do projeto.

🚀 Instruções de Execução
A execução deve seguir a ordem abaixo, pois a integração depende dos ETLs.

Passo 1: Executar os ETLs
Estes scripts leem os arquivos da pasta entradas/ e salvam os resultados em saidas/.

Script

Descrição

Saída

processador_ETL_magalu.ipynb

Processa o balancete bruto do Magalu (limpeza, padronização e ordenação).

bal_magalu_processado_[timestamp].pkl<br>bal_magalu_processado_[timestamp].xlsx

processador_ELT_epoca.ipynb

Processa o balancete da Época. É interativo para escolher o mês (atual/anterior). <br>⚠️ Executar duas vezes.

bal_epoca_[MM-YY]_[timestamp].pkl<br>bal_epoca_[MM-YY]_[timestamp].xlsx

Passo 2: Executar a Integração com Google Sheets
Após gerar os arquivos .pkl, execute o notebook integracao_sheets.ipynb.

O que faz: Orquestra a atualização final da planilha no Google Sheets.

Funcionamento (célula por célula):

💾 Conexão e Backup: Cria um backup em memória (df_backup) da planilha como um "Ctrl+Z" para restauração.

📅 Cálculo de Períodos: Gera os nomes dos meses (ex: "07-25") para buscar os arquivos.

🔗 Carregamento Dinâmico: Encontra e carrega os arquivos .pkl mais recentes de cada ETL.

🔄 Atualização Lógica: Garante a consistência e atualiza os dados na sequência correta (Anterior > Atual).

✍️ Correção de Fórmula: Insere a fórmula =CONCATENAR(...) na primeira coluna para o formato do Sheets.

✅ Envio Final: Limpa a aba de destino e cola os dados consolidados.

⏪ Restauração (Opcional): Célula no final do notebook para reverter a planilha ao estado do backup.