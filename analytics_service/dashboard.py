import os
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from sqlalchemy import create_engine, text
import humanize
import time

st.set_page_config(page_title="Dashboard de Análise de Ativos", layout="wide")
plt.style.use('seaborn-v0_8-darkgrid')


@st.cache_resource
def get_db_engine():
    try:
        db_user = os.getenv("POSTGRES_USER", "user")
        db_password = os.getenv("POSTGRES_PASSWORD", "password")
        db_host = os.getenv("POSTGRES_HOST", "postgres")
        db_port = os.getenv("PG_PORT", "5432")
        db_name = os.getenv("POSTGRES_DATABASE", "b3_dw")
        db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        return create_engine(db_url)
    except Exception as e:
        st.error(f"Erro fatal ao conectar ao banco de dados: {e}")
        return None

def check_table_exists(_engine, table_name):
    try:
        with _engine.connect() as connection:
            return connection.execute(text(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")).scalar()
    except Exception:
        return False

@st.cache_data(ttl=60)
def load_data(_engine):
    try:
        query = "SELECT * FROM b3_analytics_data WHERE date >= NOW() - INTERVAL '24 hours' ORDER BY date ASC"
        df = pd.read_sql(query, _engine)

        if df.empty:
            return pd.DataFrame()

        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df.dropna(subset=['date'], inplace=True) 

        if df['date'].dt.tz is None:
            df['date'] = df['date'].dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
        else:
            df['date'] = df['date'].dt.tz_convert('America/Sao_Paulo')
            
        return df
    except Exception as e:
        st.error(f"Ocorreu um erro ao carregar ou converter os dados de data/hora: {e}")
        return pd.DataFrame()

def main():
    st.title("Dashboard Financeiro - Análise de Ativos em Tempo Real")
    
    engine = get_db_engine()
    if engine is None: st.stop()

    if not check_table_exists(engine, 'b3_analytics_data'):
        st.info(
            "Bem-vindo ao Dashboard!\n\n"
            "Ainda não há dados para exibir.\n\n"
        )
        st.stop()
        
    initial_data = load_data(engine)
    if not initial_data.empty:
        symbols = sorted(initial_data['symbol'].unique())
        
        if 'selected_symbols' not in st.session_state:
            st.session_state.selected_symbols = symbols[:3]
        
        selected_symbols = st.multiselect(
            "Selecione os símbolos:", 
            options=symbols, 
            default=st.session_state.selected_symbols,
            key="symbol_selector"
        )
        st.session_state.selected_symbols = selected_symbols
    else:
        st.warning("Nenhum dado encontrado para carregar a lista de símbolos. Aguardando processamento...")
        st.stop()
        
    placeholder = st.empty()

    while True:
        df = load_data(engine)

        with placeholder.container():
            if df.empty:
                st.warning("Nenhum dado processado nas últimas 24 horas. Aguardando a próxima execução do pipeline...")
            
            elif not st.session_state.selected_symbols:
                st.warning("Por favor, selecione pelo menos um símbolo.")
            
            else:
                selected_symbols_from_state = st.session_state.selected_symbols
                df_filtered = df[df['symbol'].isin(selected_symbols_from_state)].copy()
                
                if df_filtered.empty:
                    st.warning("Nenhum dado encontrado para os símbolos selecionados no período recente.")
                    continue

                latest_data = df_filtered.loc[df_filtered.groupby('symbol')['date'].idxmax()]
                st.subheader("Última Cotação")
                cols = st.columns(len(selected_symbols_from_state))
                for i, symbol in enumerate(selected_symbols_from_state):
                    metric_data = latest_data[latest_data['symbol'] == symbol]
                    if not metric_data.empty:
                        price = metric_data['regularMarketPrice'].iloc[0]
                        change = metric_data['regularMarketChangePercent'].iloc[0]
                        name = metric_data['longName'].iloc[0].split(' ')[0]
                        cols[i].metric(label=f"{symbol} ({name})", value=f"R$ {price:.2f}", delta=f"{change:.2f}%")

                st.markdown("---")

                st.subheader("Evolução do Preço no Dia")
                fig_price, ax_price = plt.subplots(figsize=(14, 7))
                
                for symbol in selected_symbols_from_state:
                    df_s = df_filtered[df_filtered['symbol'] == symbol]
                    ax_price.plot(df_s['date'], df_s['regularMarketPrice'], label=symbol, marker='.', markersize=5)

                ax_price.set_ylabel("Preço (R$)")
                ax_price.set_xlabel("Horário (BRT)")
                ax_price.legend()
                ax_price.grid(True)
                ax_price.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                plt.xticks(rotation=30, ha="right")
                st.pyplot(fig_price)

                st.markdown("---")
                
                st.subheader("Análise de Mercado (Último Registro)")
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("Ativo com Maior Volume", latest_data.loc[latest_data['volume'].idxmax()]['symbol'])
                    fig_pie, ax_pie = plt.subplots()
                    market_caps = latest_data.set_index('symbol')['marketCap'].dropna()
                    if not market_caps.empty:
                        def make_autopct(values):
                            def my_autopct(pct):
                                total = sum(values)
                                val = int(round(pct*total/100.0))
                                return '{p:.1f}%  ({v})'.format(p=pct, v=humanize.intword(val, format='%.2f'))
                            return my_autopct
                        ax_pie.pie(market_caps, labels=market_caps.index, autopct=make_autopct(market_caps), startangle=90)
                        ax_pie.set_title("Distribuição de Market Cap")
                        st.pyplot(fig_pie)

                with col2:
                    st.metric("Ativo com Maior Variação (R$)", latest_data.loc[latest_data['change_day'].abs().idxmax()]['symbol'])
                    fig_vol, ax_vol = plt.subplots()
                    volume = latest_data.set_index('symbol')['volume']
                    sns.barplot(x=volume.index, y=volume.values, ax=ax_vol, palette="plasma")
                    ax_vol.set_ylabel("Nº de Ações Negociadas")
                    ax_vol.ticklabel_format(style='plain', axis='y')
                    ax_vol.set_title("Volume de Negociação")
                    ax_vol.bar_label(ax_vol.containers[0], fmt=lambda x: f'{humanize.intword(x, "%.1f")}')
                    st.pyplot(fig_vol)

                with col3:
                    st.metric("Ativo Mais Volátil no Dia", latest_data.loc[(latest_data['high'] - latest_data['low']).idxmax()]['symbol'])
                    df_range = latest_data.copy()
                    df_range['range'] = df_range['high'] - df_range['low']
                    fig_range, ax_range = plt.subplots()
                    sns.barplot(x=df_range['symbol'], y=df_range['range'], ax=ax_range, palette="magma")
                    ax_range.set_ylabel("Variação (Máxima - Mínima) em R$")
                    ax_range.set_title("Amplitude de Preço no Dia")
                    st.pyplot(fig_range)
        
        time.sleep(60)

if __name__ == "__main__":
    main()