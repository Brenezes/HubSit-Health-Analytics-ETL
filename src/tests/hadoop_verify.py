# -*- coding: utf-8 -*-
"""
Script para verificar a versão do Hadoop/Spark instalada
"""

import sys
import os

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

print("="*70)
print(" VERIFICACAO DE VERSAO DO HADOOP/SPARK")
print("="*70)

# Verificar Spark
try:
    from pyspark import SparkContext
    print(f"\n[OK] PySpark instalado")
    print(f"Versao do PySpark: {SparkContext.version}")
    
    # Criar sessão temporária para ver mais detalhes
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("VersionCheck").getOrCreate()
    
    # Pegar configurações
    conf = spark.sparkContext.getConf()
    print(f"\nConfiguracoes do Spark:")
    for item in conf.getAll():
        if 'version' in item[0].lower() or 'hadoop' in item[0].lower():
            print(f"  {item[0]}: {item[1]}")
    
    # Versão do Hadoop compilado com Spark
    hadoop_version = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    print(f"\nVersao do Hadoop (compilado com Spark): {hadoop_version}")
    
    spark.stop()
    
except Exception as e:
    print(f"[ERRO] Problema ao verificar: {str(e)}")

# Verificar caminho do Spark
print("\n" + "="*70)
print(" CAMINHOS DE INSTALACAO")
print("="*70)

spark_home = os.environ.get('SPARK_HOME', 'Nao definido')
hadoop_home = os.environ.get('HADOOP_HOME', 'Nao definido')

print(f"\nSPARK_HOME: {spark_home}")
print(f"HADOOP_HOME: {hadoop_home}")

# Verificar se winutils existe
winutils_path = "C:\\Hadoop\\bin\\winutils.exe"
if os.path.exists(winutils_path):
    print(f"\n[OK] winutils.exe encontrado em: {winutils_path}")
    print(f"Tamanho: {os.path.getsize(winutils_path) / 1024:.2f} KB")
else:
    print(f"\n[X] winutils.exe NAO encontrado em: {winutils_path}")

print("\n" + "="*70)
print(" RECOMENDACAO DE DOWNLOAD")
print("="*70)

print("\nBaixe o winutils.exe de acordo com a versao do Hadoop acima:")
print("\nRepositorio: https://github.com/cdarlint/winutils")
print("\nVersoes disponiveis:")
print("  - hadoop-2.6.0")
print("  - hadoop-2.7.1")
print("  - hadoop-2.8.1")
print("  - hadoop-3.0.0")
print("  - hadoop-3.2.0")
print("  - hadoop-3.2.1")
print("  - hadoop-3.3.1")
print("  - hadoop-3.3.5")
print("  - hadoop-3.3.6")
print("\nEscolha a versao mais proxima da sua!")