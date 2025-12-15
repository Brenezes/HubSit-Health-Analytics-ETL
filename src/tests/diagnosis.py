# -*- coding: utf-8 -*-
"""
Script de Diagnóstico para Problemas de Gravação no PySpark
Este script testa diferentes abordagens para identificar e corrigir o problema
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count
import traceback

# Configurar encoding do console para UTF-8
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

os.environ['HADOOP_HOME'] = 'C:\\Hadoop'


def testar_gravacao_simples():
    """Testa a gravação mais básica possível"""
    print("\n" + "="*70)
    print("TESTE 1: Gravacao Simples")
    print("="*70)
    
    spark = SparkSession.builder.appName("TestGravacao").getOrCreate()
    
    try:
        # Criar um DataFrame simples
        data = [("teste1", 1), ("teste2", 2), ("teste3", 3)]
        df_teste = spark.createDataFrame(data, ["coluna1", "coluna2"])
        
        print("\n[OK] DataFrame de teste criado")
        df_teste.show()
        
        # Testar gravação
        caminho = "teste_saida/teste_simples"
        print(f"\nTentando salvar em: {caminho}")
        
        df_teste.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(caminho)
        
        print("[SUCESSO] Gravacao simples funcionou!")
        return True
        
    except Exception as e:
        print(f"[ERRO] Erro na gravacao simples: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        spark.stop()


def testar_gravacao_com_encoding():
    """Testa diferentes opções de encoding"""
    print("\n" + "="*70)
    print("TESTE 2: Gravacao com Diferentes Encodings")
    print("="*70)
    
    spark = SparkSession.builder.appName("TestEncoding").getOrCreate()
    
    try:
        data = [("Joao", 1), ("Maria", 2), ("Acao", 3)]
        df_teste = spark.createDataFrame(data, ["nome", "id"])
        
        print("\n[OK] DataFrame com acentos criado")
        df_teste.show()
        
        encodings = ["UTF-8", "ISO-8859-1", "windows-1252"]
        
        for enc in encodings:
            try:
                caminho = f"teste_saida/teste_encoding_{enc.replace('-', '_')}"
                print(f"\nTentando encoding: {enc}")
                
                df_teste.coalesce(1).write.mode("overwrite") \
                    .option("header", "true") \
                    .option("encoding", enc) \
                    .csv(caminho)
                
                print(f"  [OK] {enc} funcionou!")
            except Exception as e:
                print(f"  [FALHOU] {enc} falhou: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"[ERRO] Erro geral: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        spark.stop()


def testar_gravacao_parquet():
    """Testa gravação em formato Parquet como alternativa"""
    print("\n" + "="*70)
    print("TESTE 3: Gravacao em Formato Parquet")
    print("="*70)
    
    spark = SparkSession.builder.appName("TestParquet").getOrCreate()
    
    try:
        data = [("teste1", 1), ("teste2", 2), ("teste3", 3)]
        df_teste = spark.createDataFrame(data, ["coluna1", "coluna2"])
        
        print("\n[OK] DataFrame de teste criado")
        
        # Testar Parquet
        caminho_parquet = "teste_saida/teste_parquet"
        print(f"\nTentando salvar em Parquet: {caminho_parquet}")
        
        df_teste.coalesce(1).write.mode("overwrite").parquet(caminho_parquet)
        
        print("[SUCESSO] Gravacao Parquet funcionou!")
        
        # Ler de volta
        df_lido = spark.read.parquet(caminho_parquet)
        print("\n[OK] Dados lidos do Parquet:")
        df_lido.show()
        
        return True
        
    except Exception as e:
        print(f"[ERRO] Erro: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        spark.stop()


def diagnosticar_arquivo_real():
    """Tenta carregar e analisar um pequeno pedaço do arquivo real"""
    print("\n" + "="*70)
    print("TESTE 4: Diagnostico do Arquivo Real")
    print("="*70)
    
    spark = (
        SparkSession.builder.appName("DiagnosticoReal")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    
    try:
        caminho = "base_anonima_final.csv"
        
        if not os.path.exists(caminho):
            print(f"[ERRO] Arquivo nao encontrado: {caminho}")
            return False
        
        print(f"\n[OK] Arquivo encontrado: {caminho}")
        print(f"  Tamanho: {os.path.getsize(caminho) / 1024 / 1024:.2f} MB")
        
        # Carregar apenas 100 linhas
        print("\nCarregando primeiras 100 linhas...")
        df = spark.read.csv(caminho, header=True, sep=";", encoding="utf-8").limit(100)
        
        print(f"\n[OK] Linhas carregadas: {df.count()}")
        print("\nColunas encontradas:")
        for i, col_name in enumerate(df.columns, 1):
            print(f"  {i:2}. {col_name}")
        
        # Tentar salvar uma amostra
        print("\nTentando salvar amostra...")
        caminho_saida = "teste_saida/amostra_real"
        
        df.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .option("encoding", "UTF-8") \
            .csv(caminho_saida)
        
        print("[SUCESSO] Amostra salva com sucesso!")
        return True
        
    except Exception as e:
        print(f"[ERRO] Erro: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        spark.stop()


def verificar_permissoes():
    """Verifica permissões de escrita no diretório"""
    print("\n" + "="*70)
    print("TESTE 5: Verificacao de Permissoes")
    print("="*70)
    
    try:
        # Criar pasta de teste
        pasta_teste = "teste_saida"
        
        if not os.path.exists(pasta_teste):
            os.makedirs(pasta_teste)
            print(f"[OK] Pasta criada: {pasta_teste}")
        else:
            print(f"[OK] Pasta ja existe: {pasta_teste}")
        
        # Tentar criar um arquivo de teste
        arquivo_teste = os.path.join(pasta_teste, "teste_permissao.txt")
        with open(arquivo_teste, 'w') as f:
            f.write("Teste de permissao")
        
        print(f"[OK] Arquivo de teste criado: {arquivo_teste}")
        
        # Tentar ler
        with open(arquivo_teste, 'r') as f:
            conteudo = f.read()
        
        print(f"[OK] Arquivo lido: {conteudo}")
        
        # Limpar
        os.remove(arquivo_teste)
        print("[OK] Arquivo removido")
        
        print("\n[SUCESSO] Permissoes OK!")
        return True
        
    except Exception as e:
        print(f"[ERRO] Problema de permissoes: {str(e)}")
        traceback.print_exc()
        return False


def executar_todos_testes():
    """Executa todos os testes de diagnóstico"""
    print("\n" + "="*70)
    print(" DIAGNOSTICO DE PROBLEMAS DE GRAVACAO - PySpark")
    print("="*70)
    
    resultados = {}
    
    # Teste 1
    resultados['Permissoes'] = verificar_permissoes()
    
    # Teste 2
    resultados['Gravacao Simples'] = testar_gravacao_simples()
    
    # Teste 3
    resultados['Encoding'] = testar_gravacao_com_encoding()
    
    # Teste 4
    resultados['Parquet'] = testar_gravacao_parquet()
    
    # Teste 5
    resultados['Arquivo Real'] = diagnosticar_arquivo_real()
    
    # Resumo
    print("\n" + "="*70)
    print(" RESUMO DOS TESTES")
    print("="*70)
    
    for teste, sucesso in resultados.items():
        status = "[PASSOU]" if sucesso else "[FALHOU]"
        print(f"{teste:.<40} {status}")
    
    print("\n" + "="*70)
    
    # Recomendações
    if not all(resultados.values()):
        print("\n[!] RECOMENDACOES:")
        
        if not resultados['Permissoes']:
            print("  1. Verifique as permissoes da pasta de saida")
            print("  2. Tente executar como administrador")
        
        if not resultados['Gravacao Simples']:
            print("  3. Problema fundamental com Spark/Hadoop")
            print("  4. Verifique a instalacao do Hadoop")
        
        if not resultados['Arquivo Real']:
            print("  5. Problema com o arquivo de entrada")
            print("  6. Verifique encoding e formato do CSV")
        
        print("\n  [DICA] Alternativa: Use formato Parquet ao inves de CSV")
    else:
        print("\n[SUCESSO COMPLETO] Todos os testes passaram!")
        print("\nO problema original pode ser causado por:")
        print("  1. Dados corrompidos em alguma linha especifica")
        print("  2. Memoria insuficiente para processar o arquivo completo")
        print("  3. Valores nulos/invalidos em colunas criticas")
        print("\nProximos passos:")
        print("  - Execute o script principal novamente")
        print("  - Se falhar, tente processar o arquivo em lotes menores")


if __name__ == "__main__":
    executar_todos_testes()