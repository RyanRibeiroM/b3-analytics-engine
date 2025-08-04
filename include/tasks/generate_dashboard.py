import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook

def get_real_data_from_dw():
    print("Conectando ao Data Warehouse para extrair dados...")
    
    conn = BaseHook.get_connection('postgres_dw_conn')
    
    engine_string = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(engine_string)
    
    try:
        sql_query = "SELECT * FROM b3_analytics_data;"
        df = pd.read_sql(sql_query, engine)
        
        if df.empty:
            print("AVISO: DataFrame vazio. O pipeline pode não ter carregado dados ainda.")
            return df

        print(f"Dados extraídos com sucesso! {len(df)} linhas encontradas.")
        return df
    except Exception as e:
        print(f"Erro ao carregar dados do DW: {e}")
        return pd.DataFrame()

def generate_dashboard_visuals(df):
    if df.empty:
        print("DataFrame está vazio. Não é possível gerar visualizações.")
        return

    print("\nGerando visualizações do dashboard...")
    
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['symbol', 'date'])
    
    df['daily_return'] = df.groupby('symbol')['close'].pct_change()
    df['cumulative_return'] = (1 + df['daily_return']).groupby(df['symbol']).cumprod() - 1

    output_dir = "/opt/airflow/output"
    os.makedirs(output_dir, exist_ok=True)

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(14, 8))
    for symbol in df['symbol'].unique():
        df_company = df[df['symbol'] == symbol]
        ax.plot(df_company['date'], df_company['cumulative_return'] * 100, label=symbol)
    
    ax.set_title('Retorno Acumulado das Ações', fontsize=18)
    ax.set_xlabel('Data', fontsize=12)
    ax.set_ylabel('Retorno Acumulado (%)', fontsize=12)
    ax.legend(title='Ativo')
    ax.grid(True)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/cumulative_returns.png")
    plt.close()
    print("Gráfico de Retorno Acumulado salvo em output/cumulative_returns.png")

    returns_df = df.pivot(index='date', columns='symbol', values='daily_return')
    correlation_matrix = returns_df.corr()

    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f", ax=ax, linewidths=.5)
    ax.set_title('Matriz de Correlação dos Retornos Diários', fontsize=16)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/correlation_heatmap.png")
    plt.close()
    print("Heatmap de Correlação salvo em output/correlation_heatmap.png")

def execute_dashboard_generation():
    df_data = get_real_data_from_dw()
    generate_dashboard_visuals(df_data)