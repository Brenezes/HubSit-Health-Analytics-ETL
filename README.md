HubSit Health Analytics ETL 🏥📊

Este projeto consiste em um pipeline de Engenharia de Dados desenvolvido em Python para analisar e mitigar o absenteísmo (No-Show) em clínicas médicas. O projeto foi desenvolvido como parte de uma consultoria acadêmica para a startup de saúde HubSit.


🎯 Objetivo

Transformar dados brutos e desestruturados de agendamentos ambulatoriais em inteligência de negócios, permitindo:

Identificar o perfil de pacientes propensos ao No-Show.

Calcular a Receita Potencial vs. Realizada e o impacto financeiro das faltas.

Otimizar a gestão da agenda médica através de indicadores de capacidade e ocupação.

🛠️ Tecnologias Utilizadas

Linguagem: Python 3.10+

Manipulação de Dados: Pandas, NumPy

Visualização: Power BI (alimentado pelos outputs deste script)

Conceitos: ETL (Extract, Transform, Load), Data Cleaning, Data Modeling.

⚙️ Funcionalidades do Pipeline

O script etl_pipeline.py realiza as seguintes etapas:

Ingestão: Leitura automatizada de arquivos CSV/TXT com tratamento de múltiplos encodings (UTF-8/Latin1).

Limpeza (Data Cleaning):

Normalização de nomes de médicos e pacientes.

Exclusão de agendas administrativas (Ex: Cirurgias, Testes) via blacklist.

Tratamento de valores nulos e inconsistências de datas.

Regras de Negócio:

Classificação de status: No-Show, Cancelamento Tardio (<24h) e Atendido.

Definição de Pacientes Novos vs. Recorrentes.

Criação de Faixas Etárias dinâmicas.

Exportação: Geração de tabelas dimensão e fato otimizadas na pasta data/processed/ para consumo direto no Power BI.

🚀 Como Executar

Clone este repositório:

git clone [https://github.com/Brenezes/HubSit-Health-Analytics-ETL.git](https://github.com/Brenezes/HubSit-Health-Analytics-ETL.git)


Instale as dependências:

pip install -r requirements.txt


Coloque seus arquivos de dados na pasta data/raw/ (verifique os nomes esperados no script).

Execute o pipeline:

python src/etl_pipeline.py


Os arquivos processados aparecerão em data/processed/.

📊 Estrutura dos Dados Gerados

O pipeline gera os seguintes arquivos para modelagem (Star Schema):

fato_agendamentos.csv: Base analítica completa.

financeiro.csv: Receita e ticket médio por procedimento.

perfil_noshow.csv: Taxas de falta por sexo, idade e indicação.

atravessamento.csv: Tempos de espera e pontualidade.

👨‍💻 Autor

Breno Menezes
Data Engineer | Data Analytics
LinkedIn


### Resumo do Plano de Ação:
1.  Crie a pasta do projeto no seu computador.
2.  Crie as subpastas `src`, `data`, `docs`.
3.  Mova seu código para `src` e renomeie.
4.  Crie o `requirements.txt` e o `README.md`.
5.  Dê `git init`, `git add .`, `git commit -m "Initial commit"` e suba para o GitHub.

