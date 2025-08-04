import os
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

st.set_page_config(page_title="Dashboard Financeiro", layout="wide")
st_autorefresh(interval=60 * 1000, key="data_refresh")

PG_USER = os.getenv("PG_USER", "user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "b3_dw")

engine_string = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
engine = create_engine(engine_string)

@st.cache_data(ttl=60)
def carregar_dados():
    try:
        query = 'SELECT * FROM b3_analytics_data ORDER BY "date" DESC LIMIT 1000'
        df = pd.read_sql(query, engine)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ou ler do Data Warehouse: {e}")
        return pd.DataFrame()

df = carregar_dados()

st.title("Dashboard Financeiro - Análise Completa")

if df.empty:
    st.warning("Nenhum dado disponível no Data Warehouse. Por favor, aguarde a execução do pipeline do Airflow.")
else:
    simbolos = df['symbol'].unique()
    selecionados = st.multiselect("Selecione símbolos para visualizar", options=simbolos, default=list(simbolos))

    df_filtrado = df[df['symbol'].isin(selecionados)].copy()

    df_filtrado = df_filtrado.sort_values(["symbol", "date"])
    df_filtrado['daily_return'] = df_filtrado.groupby('symbol')['close'].pct_change()
    df_filtrado['cumulative_return'] = (1 + df_filtrado['daily_return']).groupby(df_filtrado['symbol']).cumprod() - 1
    df_filtrado['sma_5'] = df_filtrado.groupby('symbol')['close'].transform(lambda x: x.rolling(window=5).mean())
    df_filtrado['sma_20'] = df_filtrado.groupby('symbol')['close'].transform(lambda x: x.rolling(window=20).mean())
    df_filtrado['volatility_5d'] = df_filtrado.groupby('symbol')['daily_return'].transform(lambda x: x.rolling(window=5).std()) * 100
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Retorno Acumulado (%)")
        fig, ax = plt.subplots(figsize=(12, 6))
        for simbolo in selecionados:
            df_s = df_filtrado[df_filtrado['symbol'] == simbolo]
            ax.plot(df_s['date'], df_s['cumulative_return'] * 100, label=simbolo) 
        ax.set_xlabel("Data")
        ax.set_ylabel("Retorno Acumulado (%)")
        ax.legend(title="Símbolo")
        ax.grid(True)
        st.pyplot(fig)

        st.subheader("Volatilidade Diária (Desvio Padrão Móvel 5 dias)")
        fig_vol, ax_vol = plt.subplots(figsize=(12, 6))
        for simbolo in selecionados:
            df_s = df_filtrado[df_filtrado['symbol'] == simbolo]
            ax_vol.plot(df_s['date'], df_s['volatility_5d'], label=simbolo)
        ax_vol.set_xlabel("Data")
        ax_vol.set_ylabel("Volatilidade (%)")
        ax_vol.legend(title="Símbolo")
        ax_vol.grid(True)
        st.pyplot(fig_vol)

    with col2:
        st.subheader("Distribuição de Retornos Diários (%)")
        fig2, axs = plt.subplots(1, len(selecionados), figsize=(5 * len(selecionados), 4), sharey=True)
        if len(selecionados) == 1:
            axs = [axs]
        for i, simbolo in enumerate(selecionados):
            df_s = df_filtrado[df_filtrado['symbol'] == simbolo]
            sns.histplot(df_s['daily_return'].dropna(), bins=50, kde=True, ax=axs[i])
            axs[i].set_title(simbolo)
            axs[i].set_xlabel("Retorno (%)")
        st.pyplot(fig2)

        st.subheader("Boxplot da Volatilidade (5 dias)")
        fig_box, ax_box = plt.subplots(figsize=(8,6))
        sns.boxplot(data=df_filtrado, x='symbol', y='volatility_5d', ax=ax_box)
        ax_box.set_ylabel("Volatilidade (%)")
        ax_box.set_xlabel("Símbolo")
        st.pyplot(fig_box)

    st.subheader("Preço de Fechamento e Médias Móveis (SMA 5 e SMA 20)")
    fig3, ax3 = plt.subplots(figsize=(14, 7))
    for simbolo in selecionados:
        df_s = df_filtrado[df_filtrado['symbol'] == simbolo]
        ax3.plot(df_s['date'], df_s['close'], label=f"{simbolo} Preço Fechamento")
        ax3.plot(df_s['date'], df_s['sma_5'], linestyle='--', label=f"{simbolo} SMA 5")
        ax3.plot(df_s['date'], df_s['sma_20'], linestyle=':', label=f"{simbolo} SMA 20")
    ax3.set_xlabel("Data")
    ax3.set_ylabel("Preço")
    ax3.legend()
    ax3.grid(True)
    st.pyplot(fig3)

    if 'turnover_ratio' in df_filtrado.columns:
        st.subheader("Taxa de Negociação (Turnover Ratio)")
        fig4, ax4 = plt.subplots(figsize=(12,6))
        for simbolo in selecionados:
            df_s = df_filtrado[df_filtrado['symbol'] == simbolo]
            ax4.plot(df_s['date'], df_s['turnover_ratio'], label=simbolo)
        ax4.set_xlabel("Data")
        ax4.set_ylabel("Turnover Ratio")
        ax4.legend()
        ax4.grid(True)
        st.pyplot(fig4)
    else:
        st.info("Coluna 'turnover_ratio' não encontrada no dataset.")

    st.subheader("Matriz de Correlação dos Retornos Diários")
    returns_df = df_filtrado.pivot(index='date', columns='symbol', values='close').pct_change() * 100 # Corrigido aqui
    corr = returns_df.corr()
    fig_corr, ax_corr = plt.subplots(figsize=(8, 6))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", ax=ax_corr)
    st.pyplot(fig_corr)