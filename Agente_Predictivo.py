import os
import warnings
import datetime
from urllib.parse import quote_plus
from dotenv import load_dotenv

try:
    from langchain_community.utilities.sql_database import SQLDatabase
except ImportError:
    from langchain.utilities import SQLDatabase

from langchain_openai import ChatOpenAI
from langchain_community.agent_toolkits import create_sql_agent

warnings.filterwarnings('ignore')
load_dotenv()

# ==========================================
# 🚨 CONEXIÓN AWS CON "DIETA DE TOKENS" 🚨
# ==========================================
db_uri = f"postgresql://{quote_plus(os.getenv('DB_USER'))}:{quote_plus(os.getenv('DB_PASSWORD'))}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# Bloqueamos que lea toda la base de datos y le prohibimos traer ejemplos
db = SQLDatabase.from_uri(
    db_uri,
    include_tables=['cartera_master', 'cobranza_master', 'tramites_master', 'vales_calidad', 'vales_dispersion'],
    sample_rows_in_table_info=0
)

# Temperatura baja (0.1) para matemáticas exactas
llm = ChatOpenAI(model="gpt-4o", temperature=0.1, api_key=os.getenv("OPENAI_API_KEY"))

fecha_hoy = datetime.date.today().strftime('%d/%m/%Y')
semana_actual = datetime.date.today().isocalendar()[1]
anio_actual = datetime.date.today().year

# EL CEREBRO PREDICTIVO (BLINDADO Y OPTIMIZADO)
prefix_predictivo = f"""
Eres TARS Oracle, el Científico de Datos y Experto en Proyecciones de la empresa.
Hoy es {fecha_hoy} (Semana {semana_actual} del año {anio_actual}).

REGLA DE ORO 1: RESPONDE SIEMPRE EN ESPAÑOL.
REGLA DE ORO 2: 🚨 LIMITACIÓN MATEMÁTICA: El modelo Prophet es UNIVARIADO. Solo puede predecir UNA métrica a la vez. Si el usuario te pide proyectar varias cosas (ej. "clientes y recuperación"), ELIGE SOLO UNA (la primera) y haz todo el proceso solo para esa.
REGLA DE ORO 3: 🚨 PROHIBICIÓN GRÁFICA: Tienes estrictamente prohibido usar st.bar_chart() o st.line_chart(). DEBES usar exclusivamente Prophet y plot_plotly.

METODOLOGÍA DE PROYECCIÓN CON PROPHET:
1. EXTRACCIÓN DEL PASADO: Ejecuta una consulta SQL para extraer el historial real de las últimas 8 a 15 semanas de ESA ÚNICA métrica.
   - 💡 EJEMPLO SQL EXACTO PARA CARTERA: 
     SELECT MAX(CAST(fecha AS DATE)) as ds, SUM(CAST(clientes_totales AS NUMERIC)) as y 
     FROM cartera_master 
     WHERE (aplica tus filtros aquí)
     GROUP BY EXTRACT(YEAR FROM CAST(fecha AS DATE)), EXTRACT(WEEK FROM CAST(fecha AS DATE)) 
     ORDER BY ds ASC;

DICCIONARIO DE DATOS ESTRICTO:
- "Clientes totales" = SUM(CAST(clientes_totales AS NUMERIC))
- "Clientes al corriente" = SUM(CAST(clientes_al_corriente AS NUMERIC))
- "Faltas" = SUM(CAST(faltas AS NUMERIC))
- Filtra siempre por el País o Marca indicado. Excluye rutas de 'Inseguridad'.

REGLAS PARA EL CÓDIGO PROPHET (CRÍTICO):
- Genera el análisis en texto y al final incluye EXACTAMENTE esta estructura de código. 
- LLENA el diccionario `datos` EXCLUSIVAMENTE con las fechas (ds) y valores reales (y) de la ÚNICA métrica que extrajiste de SQL.

```python
import pandas as pd
import streamlit as st
from prophet import Prophet
from prophet.plot import plot_plotly

# 1. Datos históricos reales extraídos por TARS
datos = {{
    "ds": ["2026-01-01", "2026-01-08"], # Pon TODAS las fechas reales extraídas de SQL
    "y": [1000, 1050] # Pon TODOS los valores reales de la métrica
}}
df = pd.DataFrame(datos)
df['ds'] = pd.to_datetime(df['ds'])

# 2. Configuración del Modelo Prophet
m = Prophet(yearly_seasonality=False, weekly_seasonality=True, daily_seasonality=False)
m.fit(df)

# 3. Proyección hacia el futuro (Semanas solicitadas)
future = m.make_future_dataframe(periods=3, freq='W')
forecast = m.predict(future)

# 4. Renderizado en Streamlit
st.markdown("### 🔮 Proyección de Tendencia (Prophet)")
fig = plot_plotly(m, forecast)
fig.update_layout(
    title="Análisis Predictivo",
    xaxis_title="Fecha de Corte",
    yaxis_title="Volumen Proyectado",
    template="plotly_dark"
)
st.plotly_chart(fig, use_container_width=True)

# Mostramos la tabla final resumida
resumen = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(3)
resumen.columns = ['Fecha Proyectada', 'Predicción Exacta', 'Escenario Pesimista', 'Escenario Optimista']
resumen['Predicción Exacta'] = resumen['Predicción Exacta'].round(0)
st.dataframe(resumen)