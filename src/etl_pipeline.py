import pandas as pd
import numpy as np
import os
import sys
import unicodedata
from datetime import datetime

# --- LISTA DE EXCLUSÃO (Agendas Administrativas/Cirúrgicas) ---
MEDICOS_IGNORAR = [

]

# Constantes de Mapeamento
MAP_STATUS_MARCACAO = {
    'A': 'Aberta',
    'E': 'Executada',
    'C': 'Cancelada',
    'B': 'Bloqueada'
}

MAP_STATUS_CONFIRMACAO = {
    'A': 'Em Aberto',
    'N': 'Não Confirmada',
    'C': 'Confirmada'
}

# Limite de pontualidade em minutos
LIMITE_PONTUALIDADE_MINUTOS = 15

# --- Funções Auxiliares ---

def normalizar_texto_chave(serie):
    """
    Normaliza textos para uso como chave de cruzamento:
    - Maiúsculas
    - Remove acentos (Á -> A)
    - Remove espaços extras
    """
    if serie is None: return None
    
    s = serie.astype(str).str.upper()
    s = s.apply(lambda x: ''.join(c for c in unicodedata.normalize('NFD', x) if unicodedata.category(c) != 'Mn'))
    s = s.str.replace(r'[^A-Z0-9 ]', '', regex=True)
    s = s.str.strip().str.replace(r'\s+', ' ', regex=True)
    
    return s

def normalizar_colunas(df):
    """
    Padroniza os nomes das colunas para garantir que o script funcione
    tanto com a base anonimizada quanto com a nominal.
    """

    mapa_colunas = {
        'Nome_Paciente': 'ID_Paciente_Anon',
        'Nome_Medico': 'ID_Medico_Anon',
        'Agendamento_Inicio': 'Agendamento Inicio',
        'Agendamento_Final': 'Agendamento Final',
        'Pacientes_Data_Registro': 'Pacientes_DataRegistro',
        'Atendimentos_Data_Hora_Chegada': 'Atendimentos_DataEHora_Chegada',
        'Atendimentos_Data_Hora_Atendimento': 'Atendimentos_DataEHora_Atendimento',
        'Atendimentos_Data_Hora_Final': 'Atendimentos_DataEHora_Final',
        'Cancelamentos_Data_Cancelamento': 'Cancelamentos_DataDeCancelamento',
        'Confirmacoes_Data_Confirmacao': 'Confirmacoes_Data_Confirmacao'
    }
    

    df = df.rename(columns=mapa_colunas)
    
    if 'ID_Paciente_Anon' not in df.columns and 'Nome_Paciente' in df.columns:
        df['ID_Paciente_Anon'] = df['Nome_Paciente']
    
    if 'ID_Medico_Anon' not in df.columns and 'Nome_Medico' in df.columns:
        df['ID_Medico_Anon'] = df['Nome_Medico']
        
    return df

def definir_turno(hora):
    if pd.isna(hora): return 'Indefinido'
    h = hora.hour
    if 6 <= h < 12: return 'MANHA'
    elif 12 <= h < 18: return 'TARDE'
    else: return 'NOITE'

def carregar_dados(csv_path, precos_path):
    print(f"Carregando base principal de: {csv_path}")
    df = None
    encodings = ['utf-8', 'cp1252', 'latin1']
    
    for encoding in encodings:
        try:
            print(f"Tentando ler base com encoding '{encoding}'...")
            df = pd.read_csv(
                csv_path,
                sep=';',
                na_values=['', ' ', 'NA', 'N/A'],
                low_memory=False,
                encoding=encoding
            )
            df = normalizar_colunas(df)
            print(f"Sucesso! Base carregada com '{encoding}'.")
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Erro ao carregar {csv_path}: {e}")
            return None, None

    if df is None:
        print("FALHA CRÍTICA: Não foi possível ler a base.")
        return None, None

    print(f"Carregando tabela de preços de: {precos_path}")
    df_precos = None
    
    separadores = [';', '\t', ',']
    
    for sep in separadores:
        for encoding in encodings:
            try:
                temp_df = pd.read_csv(
                    precos_path,
                    sep=sep,
                    decimal=',',
                    na_values=['', ' ', 'NA', 'N/A'],
                    encoding=encoding,
                    on_bad_lines='skip'
                )
                
                if temp_df.shape[1] >= 3:
                    cols_upper = [c.upper() for c in temp_df.columns]
                    
                    if 'PROCEDIMENTO' in cols_upper and 'CONVENIO' in cols_upper:
                         rename_map = {}
                         for c in temp_df.columns:
                             if c.upper().strip() == 'PROCEDIMENTO': rename_map[c] = 'Procedimento'
                             elif c.upper().strip() == 'CONVENIO': rename_map[c] = 'Convenio'
                             elif 'VALOR' in c.upper(): rename_map[c] = 'Valor_Convenio'
                         temp_df.rename(columns=rename_map, inplace=True)
                    else:
                        temp_df = temp_df.iloc[:, :3]
                        temp_df.columns = ['Procedimento', 'Convenio', 'Valor_Convenio']
                    
                    sample_val = pd.to_numeric(temp_df['Valor_Convenio'].astype(str).str.replace(',','.'), errors='coerce')
                    if sample_val.notna().sum() > 0:
                        df_precos = temp_df
                        print(f"Sucesso! Tabela de preços carregada com separador '{'TAB' if sep == '\\t' else sep}' e encoding '{encoding}'.")
                        print(f"Exemplo de carregamento: {df_precos.iloc[0].to_dict()}")
                        break 
            except Exception:
                continue 
        
        if df_precos is not None:
            break 

    if df_precos is None:
         print(f"Erro: Não foi possível carregar {precos_path}.")
         return None, None

    print("Dados carregados com sucesso.")
    return df, df_precos

def pre_processar_dados(df, df_precos):
    if df is None or df_precos is None:
        return None, {}

    print("Iniciando pré-processamento...")
    contagens_indefinidos = {}
    today = datetime.now()

    if 'ID_Medico_Anon' in df.columns:
        total_antes = len(df)
        medicos_ignorar_norm = [m.upper().strip() for m in MEDICOS_IGNORAR]
        mask_ignorar = df['ID_Medico_Anon'].astype(str).str.upper().str.strip().isin(medicos_ignorar_norm)
        
        df = df[~mask_ignorar].copy()
        
        total_depois = len(df)
        removidos = total_antes - total_depois
        print(f"\n[FILTRO] Removidos {removidos} registros de agendas administrativas/cirurgias (Lista de Exclusão).")
        print(f"Registros restantes: {total_depois}")

    colunas_data = [
        'Agendamento Inicio', 'Agendamento Final', 'Data_Marcacao',
        'Pacientes_DataNascimento', 'Pacientes_DataRegistro',
        'Confirmacoes_Data_Confirmacao', 'Confirmacoes_DataEHora_Atendimento',
        'Atendimentos_DataEHora_Chegada', 'Atendimentos_DataEHora_Registro',
        'Atendimentos_DataEHora_Atendimento', 'Atendimentos_DataEHora_Final',
        'Cancelamentos_DataDeCancelamento', 'Cancelamentos_DataEHora_Atendimento'
    ]
    
    for col in colunas_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

    if 'Pacientes_Sexo' in df.columns:
        df['Pacientes_Sexo'] = df['Pacientes_Sexo'].fillna('Indefinido')

    if 'Valor' in df.columns:
        df = df.drop(columns=['Valor'])

    if 'Procedimento' in df.columns and 'Convenio' in df.columns:
        df['key_proc'] = normalizar_texto_chave(df['Procedimento'])
        df['key_conv'] = normalizar_texto_chave(df['Convenio'])
        
        df_precos['key_proc'] = normalizar_texto_chave(df_precos['Procedimento'])
        df_precos['key_conv'] = normalizar_texto_chave(df_precos['Convenio'])
        
        print("\nRealizando cruzamento de preços...")
        
        df = pd.merge(df, df_precos[['key_proc', 'key_conv', 'Valor_Convenio']], 
                      on=['key_proc', 'key_conv'], how='left')
        
        df.rename(columns={'Valor_Convenio': 'Valor'}, inplace=True)
        df.drop(columns=['key_proc', 'key_conv'], inplace=True)
        
        if df['Valor'].dtype == 'object':
             df['Valor'] = df['Valor'].astype(str).str.replace('R$', '', regex=False) \
                                                  .str.strip() \
                                                  .str.replace('.', '', regex=False) \
                                                  .str.replace(',', '.', regex=False)
        
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
        
        qtd_nao_encontrado = df['Valor'].isna().sum()
        
        if qtd_nao_encontrado > 0:
            print(f"AVISO: {qtd_nao_encontrado} agendamentos não tiveram correspondência na Tabela de Convênio.")
            print("Exemplos de (Procedimento | Convenio) sem preço:")
            sem_preco = df[df['Valor'].isna()][['Procedimento', 'Convenio']].drop_duplicates().head(5)
            print(sem_preco.to_string(index=False))
        
        df['Valor'] = df['Valor'].fillna(0.0)

    else:
        df['Valor'] = 0.0
    
    df['agendamentos_cancelados'] = df['Cancelamentos_DataDeCancelamento'].notna()
    df['agendamentos_confirmados'] = df['Confirmacoes_Data_Confirmacao'].notna()
    df['atendimentos_ok'] = df['Atendimentos_DataEHora_Atendimento'].notna()
    df['comparecimentos_ok'] = df['Atendimentos_DataEHora_Chegada'].notna()
    
    df['horas_antecedencia_cancelamento'] = np.where(
        (df['Cancelamentos_DataDeCancelamento'].notna()) & (df['Agendamento Inicio'].notna()),
        (df['Agendamento Inicio'] - df['Cancelamentos_DataDeCancelamento']).dt.total_seconds() / 3600,
        np.nan
    )
    
    df['cancelamento_tardio'] = (
        (df['agendamentos_cancelados']) & 
        (df['horas_antecedencia_cancelamento'].notna()) & 
        (df['horas_antecedencia_cancelamento'] < 24)
    )
    
    df['noshow_padrao'] = (
        (~df['comparecimentos_ok']) &
        (df['Agendamento Inicio'] < today)
    )
    
    df['noshow_confirmado'] = (
        (df['noshow_padrao']) & 
        (df['agendamentos_confirmados'])
    )
    
    def definir_status_final(row):
        if row['atendimentos_ok']:
            return 'ATENDIDO'
        elif row['noshow_padrao']:
            return 'NO-SHOW'
        elif row['cancelamento_tardio']:
            return 'CANCELAMENTO_TARDIO'
        elif row['agendamentos_cancelados']:
            return 'CANCELADO'
        else:
            return 'AGENDADO'

    df['Status_Consolidado'] = df.apply(definir_status_final, axis=1)

    if 'Pacientes_DataNascimento' in df.columns:
        df['Idade'] = df['Pacientes_DataNascimento'].apply(
            lambda x: (today - x).days / 365.25 if pd.notna(x) else np.nan
        )
        
        bins = [0, 13, 18, 40, 60, 120]
        labels = ['0-12', '13-17', '18-39', '40-59', '60+']
        
        df['Faixa_Etaria'] = pd.cut(df['Idade'], bins=bins, labels=labels, right=False)
        df['Faixa_Etaria'] = df['Faixa_Etaria'].cat.add_categories('Indefinido').fillna('Indefinido')

    if 'Data_Marcacao' in df.columns and 'Pacientes_DataRegistro' in df.columns:
        df['is_novo'] = (df['Data_Marcacao'].dt.date == df['Pacientes_DataRegistro'].dt.date)
    else:
        df['is_novo'] = False

    if 'Agendamento Inicio' in df.columns:
        df['Turno'] = df['Agendamento Inicio'].apply(definir_turno)
        df['Data_Agendamento'] = df['Agendamento Inicio'].dt.date

    print("Pré-processamento concluído.")
    return df, contagens_indefinidos

# --- Funções de Análise ---

def formatar_relatorio(titulo, dados):
    relatorio = f"\n--- {titulo} ---\n"
    if isinstance(dados, pd.DataFrame) or isinstance(dados, pd.Series):
        relatorio += dados.to_string()
    elif isinstance(dados, dict):
        for k, v in dados.items():
            relatorio += f"{k}: {v}\n"
    else:
        relatorio += str(dados)
    relatorio += f"\n{'-' * (len(titulo) + 6)}\n"
    return relatorio

def analisar_dados_faltantes(df):
    """
    Analisa campos vazios/nulos que podem indicar falhas na base de dados.
    """
    print("\n" + "="*70)
    print(" ANALISE DE QUALIDADE DOS DADOS - DADOS FALTANTES")
    print("="*70)
    
    total_registros = len(df)
    
    colunas_criticas = [
        'Agendamento Inicio', 'Data_Marcacao', 'Valor', 'Unidade',
        'Procedimento', 'ID_Medico_Anon', 'ID_Paciente_Anon',
        'Categoria_Servico_Limpa', 'Pacientes_Sexo', 'Pacientes_DataNascimento',
        'Pacientes_Indicacao', 'Confirmacoes_Data_Confirmacao',
        'Atendimentos_DataEHora_Chegada', 'Cancelamentos_DataDeCancelamento'
    ]
    
    print("\nCampos com Dados Faltantes:")
    print("-" * 70)
    
    dados_faltantes = []
    for col in colunas_criticas:
        if col in df.columns:
            if col in ['Pacientes_Sexo', 'Pacientes_Indicacao', 'Faixa_Etaria', 'Categoria_Servico_Limpa']:
                num_nulos = (df[col].isna() | (df[col] == 'Indefinido')).sum()
            else:
                num_nulos = df[col].isna().sum()
            
            pct_nulos = (num_nulos / total_registros) * 100
            
            dados_faltantes.append({
                'Campo': col,
                'Registros_Vazios': num_nulos,
                'Percentual': round(pct_nulos, 2)
            })
    
    df_faltantes = pd.DataFrame(dados_faltantes)
    df_faltantes_print = df_faltantes[df_faltantes['Registros_Vazios'] > 0]
    
    if len(df_faltantes_print) > 0:
        print(df_faltantes_print.to_string(index=False))
        print(f"\n[!] ALERTA: {len(df_faltantes_print)} campos criticos possuem dados faltantes!")
    else:
        print("[OK] Nenhum dado faltante encontrado em campos criticos!")
    
    return df_faltantes

def analisar_noshow(df):
    df_passado = df[df['Status_Consolidado'] != 'AGENDADO'].copy()
    
    if df_passado.empty: return formatar_relatorio("1. Análise de No-Show", "Sem dados passados.")

    total = len(df_passado)
    noshows = len(df_passado[df_passado['Status_Consolidado'] == 'NO-SHOW'])
    taxa = (noshows / total) * 100 if total > 0 else 0
    
    print("\n" + "="*70)
    print(f" 1. ANALISE DE NO-SHOW (GERAL)")
    print("="*70)
    print(f"Taxa de No-Show Geral (%): {taxa:.2f}%")
    print(f"Total de Agendamentos Passados: {total}")
    print(f"Total de No-Shows: {noshows}")
    print("OBS: Agendas administrativas foram excluídas desta análise.")

    def gerar_perfil(df_in, coluna_grupo, titulo_grupo):
        if coluna_grupo not in df_in.columns: return
        
        print(f"\n--- Perfil de No-Show por {titulo_grupo} ---")
        
        df_work = df_in[df_in[coluna_grupo] != 'Indefinido'].copy()
        
        mapa_status = {
            'ATENDIDO': 'Realizado',
            'NO-SHOW': 'No-Show',
            'CANCELADO': 'Cancelado',
            'CANCELAMENTO_TARDIO': 'Cancelado'
        }
        df_work['Status_Simples'] = df_work['Status_Consolidado'].map(mapa_status).fillna('Outro')
        
        tabela = pd.crosstab(df_work[coluna_grupo], df_work['Status_Simples'])
        
        for c in ['No-Show', 'Realizado', 'Cancelado']:
            if c not in tabela.columns: tabela[c] = 0
            
        tabela = tabela[['No-Show', 'Realizado', 'Cancelado']]
        tabela['total'] = tabela.sum(axis=1)
        tabela['taxa_no_show_%'] = (tabela['No-Show'] / tabela['total'] * 100).round(2)
        
        if not tabela.empty:
            if titulo_grupo == 'INDICACAO':
                print(tabela.sort_values('total', ascending=False).to_string())
            else:
                print(tabela.to_string())

    gerar_perfil(df_passado, 'Pacientes_Sexo', 'SEXO')
    gerar_perfil(df_passado, 'Faixa_Etaria', 'FAIXA ETARIA')
    gerar_perfil(df_passado, 'Pacientes_Indicacao', 'INDICACAO')

    return ""

def calcular_receita_potencial_e_ticket(df):
    df_executados = df[df['Status_Consolidado'] == 'ATENDIDO']
    
    receita_realizada = df_executados['Valor'].sum()
    total_executados = len(df_executados)
    
    ticket_medio = (receita_realizada / total_executados) if total_executados > 0 else 0

    df_perda = df[df['Status_Consolidado'].isin(['NO-SHOW'])]
    receita_perdida = df_perda['Valor'].sum()
    
    receita_potencial = receita_realizada + receita_perdida
    taxa_realizacao = (receita_realizada / receita_potencial) * 100 if receita_potencial > 0 else 0

    relatorio = {
        "Ticket Médio (Realizado)": f"R$ {ticket_medio:,.2f}",
        "Receita Realizada (ATENDIDO)": f"R$ {receita_realizada:,.2f}",
        "Receita Perdida (NO-SHOW)": f"R$ {receita_perdida:,.2f}",
        "Receita Potencial Total": f"R$ {receita_potencial:,.2f}",
        "Taxa de Realização Financeira": f"{taxa_realizacao:.2f}%"
    }
    return formatar_relatorio("2 e 7. Financeiro (Receita e Ticket)", relatorio)

def calcular_antecedencia(df):
    if 'Agendamento Inicio' not in df.columns or 'Data_Marcacao' not in df.columns: return ""
    df_val = df.dropna(subset=['Agendamento Inicio', 'Data_Marcacao']).copy()
    df_val['Antecedencia'] = (df_val['Agendamento Inicio'] - df_val['Data_Marcacao']).dt.total_seconds() / 86400
    df_val = df_val[df_val['Antecedencia'] >= 0]
    return formatar_relatorio("4. Antecedência", {"Média (Dias)": f"{df_val['Antecedencia'].mean():.2f}"})

def analisar_atravessamento(df):
    cols = ['Atendimentos_DataEHora_Chegada', 'Atendimentos_DataEHora_Atendimento', 'Atendimentos_DataEHora_Final']
    if not all(c in df.columns for c in cols): return ""
    
    df_j = df[df['atendimentos_ok'] & df[cols].notna().all(axis=1)].copy()
    if df_j.empty: return ""

    df_j['Tempo_Total'] = (df_j['Atendimentos_DataEHora_Final'] - df_j['Atendimentos_DataEHora_Chegada']).dt.total_seconds() / 60
    df_j['Espera'] = (df_j['Atendimentos_DataEHora_Atendimento'] - df_j['Atendimentos_DataEHora_Chegada']).dt.total_seconds() / 60
    
    return formatar_relatorio("8. Jornada", {
        "Tempo Total Médio (Min)": f"{df_j['Tempo_Total'].mean():.2f}",
        "Espera Média (Min)": f"{df_j['Espera'].mean():.2f}"
    })

# --- GERAÇÃO DE TABELAS AGREGADAS PARA POWER BI ---

def gerar_tabelas_agregadas_powerbi(df, output_dir="data/processed"):
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    print(f"\nGerando tabelas otimizadas em '{output_dir}'...")

    print("0. Salvando base_tratada_completa.csv (Base Analítica)...")
    cols_to_save = [c for c in df.columns if not c.startswith('key_')]
    df[cols_to_save].to_csv(os.path.join(output_dir, "base_tratada_completa.csv"), index=False, sep=';', decimal=',')

    df_passado = df[df['Status_Consolidado'] != 'AGENDADO'].copy()

    print("1. Gerando agenda_comparecimento.csv...")
    if 'Data_Agendamento' in df.columns:
        agg_dia = df.groupby('Data_Agendamento').agg(
            total_agendado=('Status_Consolidado', 'count'),
            total_realizado=('Status_Consolidado', lambda x: (x == 'ATENDIDO').sum()),
            total_no_show=('Status_Consolidado', lambda x: (x == 'NO-SHOW').sum()),
            total_cancelado=('Status_Consolidado', lambda x: x.isin(['CANCELADO', 'CANCELAMENTO_TARDIO']).sum())
        ).reset_index()
        
        agg_dia['taxa_no_show_%'] = (agg_dia['total_no_show'] / agg_dia['total_agendado'] * 100).fillna(0).round(2)
        agg_dia['taxa_cancelamento_%'] = (agg_dia['total_cancelado'] / agg_dia['total_agendado'] * 100).fillna(0).round(2)
        agg_dia['taxa_realizacao_%'] = (agg_dia['total_realizado'] / agg_dia['total_agendado'] * 100).fillna(0).round(2)
        
        agg_dia.to_csv(os.path.join(output_dir, "agenda_comparecimento.csv"), index=False, sep=';', decimal=',')

    print("2. Gerando status_por_turno.csv...")
    if 'Turno' in df.columns:
        agg_turno = df_passado.groupby('Turno', observed=False).agg(
            total_agendado=('Status_Consolidado', 'count'),
            atendido=('Status_Consolidado', lambda x: (x == 'ATENDIDO').sum()),
            no_show=('Status_Consolidado', lambda x: (x == 'NO-SHOW').sum()),
            cancelado=('Status_Consolidado', lambda x: (x == 'CANCELADO').sum()),
            cancelamento_tardio=('Status_Consolidado', lambda x: (x == 'CANCELAMENTO_TARDIO').sum())
        ).reset_index()
        agg_turno['taxa_no_show_%'] = (agg_turno['no_show'] / agg_turno['total_agendado'] * 100).fillna(0).round(2)
        agg_turno.to_csv(os.path.join(output_dir, "status_por_turno.csv"), index=False, sep=';', decimal=',')

    print("3. Gerando perfil_noshow.csv...")
    dimensoes = {
        'Pacientes_Sexo': 'Sexo',
        'Faixa_Etaria': 'Faixa_Etaria',
        'Pacientes_Indicacao': 'Indicacao'
    }
    
    dfs_perfil = []
    for col_df, nome_dim in dimensoes.items():
        if col_df in df_passado.columns:
            temp = df_passado[df_passado[col_df] != 'Indefinido'].copy()
            if not temp.empty:
                grp = temp.groupby(col_df, observed=False).agg(
                    No_Show=('Status_Consolidado', lambda x: (x == 'NO-SHOW').sum()),
                    Realizado=('Status_Consolidado', lambda x: (x == 'ATENDIDO').sum()),
                    Cancelado=('Status_Consolidado', lambda x: x.isin(['CANCELADO', 'CANCELAMENTO_TARDIO']).sum()),
                    total=('Status_Consolidado', 'count')
                ).reset_index()
                
                grp['dimensao'] = nome_dim
                grp.rename(columns={col_df: 'valor_dimensao', 'No_Show': 'No-Show'}, inplace=True)
                grp['taxa_no_show_%'] = (grp['No-Show'] / grp['total'] * 100).fillna(0).round(2)
                dfs_perfil.append(grp)
    
    if dfs_perfil:
        perfil_final = pd.concat(dfs_perfil)
        colunas_ordem = ['dimensao', 'valor_dimensao', 'No-Show', 'Realizado', 'Cancelado', 'total', 'taxa_no_show_%']
        perfil_final[colunas_ordem].to_csv(os.path.join(output_dir, "perfil_noshow.csv"), index=False, sep=';', decimal=',')

    print("4. Gerando financeiro.csv...")
    cols_fin = ['Unidade', 'Procedimento']
    if all(c in df.columns for c in cols_fin):
        agg_fin = df_passado.groupby(cols_fin, observed=False).agg(
            qtde_agendamentos=('Status_Consolidado', 'count'),
            qtde_realizados=('Status_Consolidado', lambda x: (x == 'ATENDIDO').sum()),
            qtde_no_show=('Status_Consolidado', lambda x: (x == 'NO-SHOW').sum()),
            receita_realizada=('Valor', lambda x: x[df_passado['Status_Consolidado'] == 'ATENDIDO'].sum()),
            receita_perdida_no_show=('Valor', lambda x: x[df_passado['Status_Consolidado'] == 'NO-SHOW'].sum()),
            receita_perdida_cancelado=('Valor', lambda x: x[df_passado['Status_Consolidado'].isin(['CANCELADO', 'CANCELAMENTO_TARDIO'])].sum()),
            receita_potencial=('Valor', 'sum') 
        ).reset_index()
        
        agg_fin['ticket_medio'] = (agg_fin['receita_realizada'] / agg_fin['qtde_realizados']).fillna(0).round(2)
        agg_fin.to_csv(os.path.join(output_dir, "financeiro.csv"), index=False, sep=';', decimal=',')

    print("5. Gerando atravessamento.csv...")
    cols_atrav = ['Atendimentos_DataEHora_Chegada', 'Atendimentos_DataEHora_Atendimento', 'Atendimentos_DataEHora_Final']
    if all(c in df.columns for c in cols_atrav):
        df_atend = df[(df['Status_Consolidado'] == 'ATENDIDO') & df[cols_atrav].notna().all(axis=1)].copy()
        
        df_atend['tempo_total'] = (df_atend['Atendimentos_DataEHora_Final'] - df_atend['Atendimentos_DataEHora_Chegada']).dt.total_seconds() / 60
        df_atend['tempo_espera'] = (df_atend['Atendimentos_DataEHora_Atendimento'] - df_atend['Atendimentos_DataEHora_Chegada']).dt.total_seconds() / 60
        
        df_atend['pontualidade'] = (df_atend['Atendimentos_DataEHora_Atendimento'] - df_atend['Agendamento Inicio']).dt.total_seconds() / 60
        df_atend['no_horario'] = df_atend['pontualidade'] <= LIMITE_PONTUALIDADE_MINUTOS
        
        grp_atrav = df_atend.groupby(['Unidade', 'ID_Medico_Anon'], observed=False).agg(
            qtde_atendimentos=('Status_Consolidado', 'count'),
            tempo_medio_total_min=('tempo_total', 'mean'),
            tempo_medio_espera_min=('tempo_espera', 'mean'),
            pontualidade_media_min=('pontualidade', 'mean'),
            atendimentos_no_horario=('no_horario', 'sum')
        ).reset_index()
        
        grp_atrav['taxa_pontualidade_%'] = (grp_atrav['atendimentos_no_horario'] / grp_atrav['qtde_atendimentos'] * 100).fillna(0).round(2)
        cols_round = ['tempo_medio_total_min', 'tempo_medio_espera_min', 'pontualidade_media_min']
        grp_atrav[cols_round] = grp_atrav[cols_round].round(2)
        grp_atrav.to_csv(os.path.join(output_dir, "atravessamento.csv"), index=False, sep=';', decimal=',')

    print("6. Gerando fluxo_pacientes_agregado.csv...")
    if 'is_novo' in df_passado.columns:
        df_passado['tipo_paciente_txt'] = df_passado['is_novo'].map({True: 'Novo', False: 'Recorrente'})
        grp_fluxo = df_passado['tipo_paciente_txt'].value_counts().reset_index()
        grp_fluxo.columns = ['tipo_paciente', 'quantidade']
        total_pacs = grp_fluxo['quantidade'].sum()
        grp_fluxo['percentual_%'] = (grp_fluxo['quantidade'] / total_pacs * 100).round(2)
        grp_fluxo.to_csv(os.path.join(output_dir, "fluxo_pacientes_agregado.csv"), index=False, sep=';', decimal=',')

    print("7. Gerando indicadores_confirmacao.csv...")
    total_geral = len(df)
    resumo = {
        'Total Agendamentos': total_geral,
        'Agendamentos Confirmados': df['agendamentos_confirmados'].sum(),
        'Comparecimentos': df['comparecimentos_ok'].sum(),
        'Atendimentos Realizados': (df['Status_Consolidado'] == 'ATENDIDO').sum(),
        'No-Shows': (df['Status_Consolidado'] == 'NO-SHOW').sum(),
        'Cancelamentos': df['agendamentos_cancelados'].sum(),
        'Cancelamentos Tardios': df['cancelamento_tardio'].sum()
    }
    
    lista_resumo = []
    for k, v in resumo.items():
        pct = (v / total_geral * 100) if total_geral > 0 else 0
        lista_resumo.append({'indicador': k, 'quantidade': v, 'percentual': round(pct, 2)})
    
    pd.DataFrame(lista_resumo).to_csv(os.path.join(output_dir, "indicadores_confirmacao.csv"), index=False, sep=';', decimal=',')

    print("8. Gerando qualidade_dados.csv...")
    colunas_validar = [
        'Agendamento Inicio', 'Data_Marcacao', 'Valor', 'Unidade',
        'Procedimento', 'ID_Medico_Anon', 'ID_Paciente_Anon',
        'Categoria_Servico_Limpa', 'Pacientes_Sexo', 'Pacientes_DataNascimento',
        'Pacientes_Indicacao', 'Confirmacoes_Data_Confirmacao'
    ]
    
    dados_qualidade = []
    total_regs = len(df)
    for col in colunas_validar:
        if col in df.columns:
            if df[col].dtype == 'object':
                vazios = (df[col].isna() | (df[col] == 'Indefinido')).sum()
            else:
                vazios = df[col].isna().sum()
                
            preenchidos = total_regs - vazios
            dados_qualidade.append({
                'campo': col,
                'total_registros': total_regs,
                'registros_vazios': vazios,
                'registros_preenchidos': preenchidos,
                'percentual_preenchimento': round(preenchidos/total_regs*100, 2),
                'percentual_vazios': round(vazios/total_regs*100, 2)
            })
    
    pd.DataFrame(dados_qualidade).to_csv(os.path.join(output_dir, "qualidade_dados.csv"), index=False, sep=';', decimal=',')
    
    print("9. Gerando perfil_agenda.csv (Com Ocupação)...")

    grp_agenda = df_passado.groupby(['Unidade', 'ID_Medico_Anon', 'Procedimento', 'Categoria_Servico'], observed=False).agg(
        qtde_agendamentos=('Status_Consolidado', 'count'),
        qtde_realizados=('Status_Consolidado', lambda x: (x == 'ATENDIDO').sum()),
        qtde_no_show=('Status_Consolidado', lambda x: (x == 'NO-SHOW').sum()),
        qtde_cancelados=('Status_Consolidado', lambda x: x.isin(['CANCELADO', 'CANCELAMENTO_TARDIO']).sum()),
        valor_total_agendado=('Valor', 'sum')
    ).reset_index()
    
    file_ocupacao = "OcupacaoAgenda.csv"
    if os.path.exists(file_ocupacao):
        print(f"   Carregando {file_ocupacao}...")
        try:
            df_ocup = None
            for sep in [';', ',', '\t']:
                try:
                    tmp = pd.read_csv(file_ocupacao, sep=sep, encoding='latin1')
                    if 'Nome_Medico' in tmp.columns and 'qtde_horarios_disponiveis' in tmp.columns:
                        df_ocup = tmp
                        break
                    tmp = pd.read_csv(file_ocupacao, sep=sep, encoding='utf-8')
                    if 'Nome_Medico' in tmp.columns and 'qtde_horarios_disponiveis' in tmp.columns:
                        df_ocup = tmp
                        break
                except: continue
            
            if df_ocup is not None:
                df_ocup['key_medico'] = normalizar_texto_chave(df_ocup['Nome_Medico'])
                grp_agenda['key_medico'] = normalizar_texto_chave(grp_agenda['ID_Medico_Anon'])
                
                ocup_agrupada = df_ocup.groupby('key_medico')['qtde_horarios_disponiveis'].sum().reset_index()
                
                grp_agenda = pd.merge(grp_agenda, ocup_agrupada, on='key_medico', how='left')
                grp_agenda['qtde_horarios_disponiveis'] = grp_agenda['qtde_horarios_disponiveis'].fillna(0).astype(int)

                grp_agenda.drop(columns=['key_medico'], inplace=True)
            else:
                print("   AVISO: Colunas 'Nome_Medico' ou 'qtde_horarios_disponiveis' não encontradas em OcupacaoAgenda.csv.")
                grp_agenda['qtde_horarios_disponiveis'] = 0
        except Exception as e:
            print(f"   ERRO ao ler OcupacaoAgenda.csv: {e}")
            grp_agenda['qtde_horarios_disponiveis'] = 0
    else:
        print("   AVISO: OcupacaoAgenda.csv não encontrado. Coluna preenchida com 0.")
        grp_agenda['qtde_horarios_disponiveis'] = 0

    grp_agenda['ticket_medio'] = (grp_agenda['valor_total_agendado'] / grp_agenda['qtde_agendamentos']).fillna(0).round(2)
    grp_agenda['taxa_realizacao_%'] = (grp_agenda['qtde_realizados'] / grp_agenda['qtde_horarios_disponiveis'] * 100).fillna(0).round(2)
    grp_agenda['taxa_ocupacao_%'] = (grp_agenda['qtde_agendamentos'] / grp_agenda['qtde_horarios_disponiveis'] * 100).fillna(0).round(2)
    
    cols = [
        'Unidade', 'ID_Medico_Anon', 'Procedimento', 'Categoria_Servico',
        'qtde_agendamentos', 'qtde_horarios_disponiveis', 'qtde_realizados', 
        'qtde_no_show', 'qtde_cancelados', 'valor_total_agendado', 
        'ticket_medio', 'taxa_realizacao_%', 'taxa_ocupacao_%'
    ]
    grp_agenda = grp_agenda[cols]
    
    grp_agenda.to_csv(os.path.join(output_dir, "perfil_agenda.csv"), index=False, sep=';', decimal=',')
    
    print("\n[SUCESSO] Todas as tabelas agregadas foram geradas na pasta 'dados_para_powerbi'.")

# --- Main ---

def main():
    if len(sys.argv) >= 3:
        base_csv, precos_txt = sys.argv[1], sys.argv[2]
    else:
        base_csv, precos_txt = 'base_anonima_final.csv', 'TabelaConvenio.txt'

    df, df_precos = carregar_dados(base_csv, precos_txt)
    
    if df is not None:
        df_proc, _ = pre_processar_dados(df, df_precos)
        
        print(analisar_dados_faltantes(df_proc))
        print(analisar_noshow(df_proc))
        print(calcular_receita_potencial_e_ticket(df_proc))
        print(calcular_antecedencia(df_proc))
        print(analisar_atravessamento(df_proc))
        
        gerar_tabelas_agregadas_powerbi(df_proc)

if __name__ == "__main__":
    main()