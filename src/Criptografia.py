import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['HADOOP_USER_NAME'] = 'Breno'
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat, col, upper, trim, regexp_replace, when, lit

# =========================================================
# 1. FUNÇÃO CENTRAL DE PSEUDONIMIZAÇÃO
# =========================================================

def pseudonimizar_dados(df, nome_coluna_principal, nome_coluna_id, nome_coluna_secundaria=None):
    """
    Criptografa (pseudonimiza) uma coluna ou a concatenação de duas colunas
    usando o algoritmo SHA-256.

    Args:
        df (DataFrame): O DataFrame PySpark.
        nome_coluna_principal (str): Nome da coluna primária (Ex: Nome do paciente).
        nome_coluna_id (str): Nome da nova coluna de ID a ser criada.
        nome_coluna_secundaria (str, opcional): Nome da coluna secundária (Ex: CPF).

    Returns:
        DataFrame: O DataFrame com a nova coluna de ID criptografada.
    """
    if nome_coluna_secundaria:
        print(f"-> Criptografando '{nome_coluna_principal}' e '{nome_coluna_secundaria}' juntos...")

        return df.withColumn(
            nome_coluna_id,
            sha2(
                concat(col(nome_coluna_principal), col(nome_coluna_secundaria)),
                256  
            )
        )
    else:
        print(f"-> Criptografando apenas '{nome_coluna_principal}'...")

        return df.withColumn(
            nome_coluna_id,
            sha2(col(nome_coluna_principal), 256)
        )


# =========================================================
# 2. CONFIGURAÇÃO E CARREGAMENTO DO SPARK
# =========================================================

spark = SparkSession.builder \
    .appName("PseudonimizacaoBaseConsolidada") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.driver.extraJavaOptions", ...) \
    .config("spark.executor.extraJavaOptions", ...) \
    .getOrCreate()

try:
    df = spark.read.csv(
        "Base consolidada.CSV",
        sep=';',
        header=True,
        inferSchema=True,
        encoding = 'ISO-8859-1'
    )
    print("CSV carregado com sucesso.")
except Exception as e:
    print(f"Erro ao carregar o CSV: {e}")
    exit()

# =========================================================
# 3. PRÉ-TRATAMENTO (LIMPEZA DE DADOS)
# =========================================================

df = df.withColumn("Nome_Paciente_LIMPO", upper(trim(col("Nome do paciente"))))
df = df.withColumn("Nome_Medico_LIMPO", upper(trim(col("Nome do médico"))))
df = df.withColumn("CPF_LIMPO",
                    when(col("CPF").isNull(), lit("CPF_NULO"))  
                   .otherwise(col("CPF").cast("string"))
                   )

df = df.withColumn("CPF_LIMPO", regexp_replace(col("CPF_LIMPO"), r"\.0$", ""))

df = df.withColumnRenamed("Convênio", "Convenio")
df = df.withColumnRenamed("Data da marcação", "Data_Marcacao")
df = df.withColumnRenamed("Status da marcação", "Status_Marcacao")
df = df.withColumnRenamed("Usuário responsável", "Usuario_Responsavel")
df = df.withColumnRenamed("Categoria do Serviço", "Categoria_Servico")
df = df.withColumnRenamed("Pacientes.Sexo", "Pacientes_Sexo")
df = df.withColumnRenamed("Pacientes.Data de nascimento", "Pacientes_DataNascimento")
df = df.withColumnRenamed("Pacientes.Indicação", "Pacientes_Indicacao")
df = df.withColumnRenamed("Pacientes.Data de registro", "Pacientes_DataRegistro")
df = df.withColumnRenamed("Pacientes.Usuário que registrou", "Pacientes_UsuarioRegistrou")
df = df.withColumnRenamed("Confirmacoes.Data da confirmação", "Confirmacoes_Data_Confirmacao")
df = df.withColumnRenamed("Confirmacoes.Status da confirmação","Confirmacoes_Status_Confirmacao")
df = df.withColumnRenamed("Confirmacoes.Usuário que confirmou","Confirmacoes_Usuario_Confirmou")
df = df.withColumnRenamed("Confirmacoes.Status de execução","Confirmacoes_Status_Execucao")
df = df.withColumnRenamed("Confirmacoes.Data e hora do atendimento","Confirmacoes_DataEHora_Atendimento")
df = df.withColumnRenamed("Atendimentos.Data e hora chegada","Atendimentos_DataEHora_Chegada")
df = df.withColumnRenamed("Atendimentos.Data e hora registro","Atendimentos_DataEHora_Registro")
df = df.withColumnRenamed("Atendimentos.Data e hora do atendimento","Atendimentos_DataEHora_Atendimento")
df = df.withColumnRenamed("Atendimentos.Data e hora final","Atendimentos_DataEHora_Final")
df = df.withColumnRenamed("Atendimentos.Status do Atendimento","Atendimentos_Status_Atendimento")
df = df.withColumnRenamed("Cancelamentos.Data de cancelamento","Cancelamentos_DataDeCancelamento")
df = df.withColumnRenamed("Cancelamentos.Usuário que cancelou","Cancelamentos_Usuario_Cancelou")
df = df.withColumnRenamed("Cancelamentos.Status de execução","Cancelamentos_Status_Execucao")
df = df.withColumnRenamed("Cancelamentos.Data e hora de atendimento","Cancelamentos_DataEHora_Atendimento")

# =========================================================
# 4. EXECUÇÃO DA PSEUDONIMIZAÇÃO SIMULTÂNEA
# =========================================================

print("\n--- INICIANDO PSEUDONIMIZAÇÃO ---")

# 4.1. Pseudonimizar PACIENTES (Nome + CPF)
df_anon = pseudonimizar_dados(
    df=df,
    nome_coluna_principal="Nome_Paciente_LIMPO",  # Coluna LIMPA
    nome_coluna_id="ID_Paciente_Anon",
    nome_coluna_secundaria="CPF_LIMPO"  # Coluna LIMPA
)

# 4.2. Pseudonimizar MÉDICOS (Apenas Nome)
df_anon = pseudonimizar_dados(
    df=df_anon,
    nome_coluna_principal="Nome_Medico_LIMPO",  # Coluna LIMPA
    nome_coluna_id="ID_Medico_Anon"
)

print("\n--- PSEUDONIMIZAÇÃO CONCLUÍDA ---")

# =========================================================
# 5. RESULTADO FINAL E LIMPEZA
# =========================================================

colunas_finais = [
    "Unidade",
    "Procedimento",
    "ID_Medico_Anon",
    "ID_Paciente_Anon",
    "Convenio",
    "Valor",
    "Agendamento Inicio",
    "Agendamento Final",
    "Data_Marcacao",
    "Status_Marcacao",
    "Usuario_Responsavel",
    "Categoria_Servico",
    "Bloqueio",
    "Pacientes_Sexo",
    "Pacientes_DataNascimento",
    "Pacientes_Indicacao",
    "Pacientes_DataRegistro",
    "Pacientes_UsuarioRegistrou",
    "Confirmacoes_Data_Confirmacao",
    "Confirmacoes_Status_Confirmacao",
    "Confirmacoes_Usuario_Confirmou",
    "Confirmacoes_Status_Execucao",
    "Confirmacoes_DataEHora_Atendimento",
    "Atendimentos_DataEHora_Chegada",
    "Atendimentos_DataEHora_Registro",
    "Atendimentos_DataEHora_Atendimento",
    "Atendimentos_DataEHora_Final",
    "Atendimentos_Status_Atendimento",
    "Cancelamentos_DataDeCancelamento",
    "Cancelamentos_Usuario_Cancelou",
    "Cancelamentos_Status_Execucao",
    "Cancelamentos_DataEHora_Atendimento"
]

print("\nPrimeiras 5 linhas do DataFrame Anônimo (incluindo originais para comparação):")
df_anon.select(colunas_finais).show(5, truncate=False)
df_final_para_consultoria = df_anon.select(colunas_finais)
print("-> Coletando DataFrame para Python para salvar via Pandas...")

# 1. COLETAR: 
df_pandas = df_final_para_consultoria.toPandas()

# 2. SALVAR: 
df_pandas.to_csv(
    "base_anonima_final.csv", 
    sep=';', 
    index=False,
    encoding='utf-8'
)

print("\n--- SALVAMENTO CONCLUÍDO COM SUCESSO via Pandas ---")
# ===========================================================

spark.stop()