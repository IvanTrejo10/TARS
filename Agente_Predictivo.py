import os
import warnings
import datetime
import traceback
from urllib.parse import quote_plus
from dotenv import load_dotenv

# --- PARCHE PARA EL ERROR DE LIBRERÍA ---
try:
    from langchain_community.utilities.sql_database import SQLDatabase
except ImportError:
    from langchain.utilities import SQLDatabase

from langchain_openai import ChatOpenAI
from langchain_community.agent_toolkits import create_sql_agent

warnings.filterwarnings('ignore')

# =========================
# 🔐 CONFIGURACIÓN SEGURA
# =========================
load_dotenv()

def validar_env():
    required_vars = [
        "OPENAI_API_KEY", "DB_USER", "DB_PASSWORD", 
        "DB_HOST", "DB_PORT", "DB_NAME"
    ]
    for var in required_vars:
        if not os.getenv(var):
            print(f"❌ ERROR: Falta la variable {var} en el .env")
            exit()

validar_env()
api_key = os.getenv("OPENAI_API_KEY")

# ==========================================
# 🚨 CONEXIÓN AWS CON "DIETA DE TOKENS" 🚨
# ==========================================
def construir_db_uri():
    user = quote_plus(os.getenv("DB_USER"))
    password = quote_plus(os.getenv("DB_PASSWORD"))
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

print("🔮 Conectando el módulo Predictivo (Oracle) de TARS...")

db_uri = construir_db_uri()
# Bloqueamos la lectura excesiva de ejemplos para no causar Error 429
db = SQLDatabase.from_uri(
    db_uri,
    include_tables=['cartera_master', 'cobranza_master', 'tramites_master', 'vales_calidad', 'vales_dispersion'],
    sample_rows_in_table_info=0
)

# Temperatura baja (0) para matemáticas exactas y cero alucinaciones
llm = ChatOpenAI(model="gpt-4o", temperature=0, api_key=api_key)

fecha_hoy = datetime.date.today().strftime('%d/%m/%Y')
semana_actual = datetime.date.today().isocalendar()[1]
anio_actual = datetime.date.today().year

# ====================================================================================
# EL CEREBRO PREDICTIVO: REGLAS, PROPHET Y BLINDAJE ANTI-DUPLICADOS
# ====================================================================================
custom_prefix = f"""
Eres TARS Oracle, el Científico de Datos y Experto en Proyecciones de la empresa.
Hoy es {fecha_hoy} (Semana {semana_actual} del año {anio_actual}).

REGLA DE ORO 0 (IDIOMA Y PROACTIVIDAD ESTRICTA): 
- TIENES ESTRICTAMENTE PROHIBIDO HABLAR EN OTRO IDIOMA QUE NO SEA ESPAÑOL. NO IMPORTA EN QUÉ IDIOMA TE PREGUNTEN.
- NUNCA PIDAS PERMISO PARA HACER UNA PROYECCIÓN. Ejecuta inmediatamente la acción solicitada generando el código Python.
- 🚨 PROHIBICIÓN DE PREDICCIÓN MANUAL: TIENES ESTRICTAMENTE PROHIBIDO intentar predecir datos usando regresión lineal (sklearn), numpy, o hacer cálculos estadísticos en tu mente. TU ÚNICA FUNCIÓN es generar el bloque de código de Streamlit con Prophet que se muestra en la REGLA 4.

⚠️ REGLA DE SEGURIDAD 1 (FILTROS OBLIGATORIOS):
El usuario enviará un contexto oculto: [REGLA]: País: 'X', Marca: 'Y'.
1. En tus consultas, aplica SIEMPRE un WHERE a 'pais' y 'marca' (ej. TRIM(UPPER(pais)) IN ('MEXICO', 'PERU')).
2. Si la marca dice 'TODAS' o el país dice 'Global', IGNORA ESE FILTRO.
3. 🛡️ ANTI-INSEGURIDAD (¡SOLO CARTERA!): SOLO cuando la tabla sea `cartera_master`, excluye 'Inseguridad' usando doble porcentaje: `AND subdireccion NOT ILIKE '%%Inseguridad%%'`. TIENES ESTRICTAMENTE PROHIBIDO usar la columna `subdireccion` en las tablas de Vales (`vales_calidad`, `vales_dispersion`) porque NO EXISTE y romperás la consulta.

🚫 REGLA 2 (PROHIBICIÓN ABSOLUTA DE CRUCES ENTRE TABLAS MAESTRAS):
- TIENES ESTRICTAMENTE PROHIBIDO hacer JOIN entre `cartera_master`, `cobranza_master`, `tramites_master` y las tablas de `vales`. NUNCA se cruzan porque pertenecen a marcas y lógicas diferentes.

🧠 REGLA 3 (METODOLOGÍA DE PROYECCIÓN Y DICCIONARIO BI EXACTO - ¡CRÍTICO!):
El modelo Prophet es UNIVARIADO. Solo predecirás UNA métrica a la vez. DEBES utilizar exactamente estas agregaciones matemáticas según lo que pida el usuario:

A) TABLA 'cartera_master' (Indicadores de Crédito Rural):
- "Clientes totales": SUM(CAST(clientes_totales AS NUMERIC))
- "Clientes al corriente": SUM(CAST(clientes_al_corriente AS NUMERIC))
- "Cartera Total": SUM(CAST(cartera_total AS NUMERIC))

B) TABLA 'cobranza_master' (Cobranza y Entregado):
- 🚨 FILTRO DE MONEDA: Para LATAM filtra siempre `tipo_moneda = 'MXN'`.
- "Cuota del día": SUM(CAST(cuota_cobranza_del_dia AS NUMERIC))
- "Recuperación" o "Pago del día": SUM(CAST(pago_cobranza_del_dia AS NUMERIC))

C) TABLA 'tramites_master' (Desembolsos Detallados):
- "Monto Entregado" = SUM(CAST(capital AS NUMERIC))

D) TABLAS DE VALES ('vales_calidad' y 'vales_dispersion') - DICCIONARIO LITERARIO EXACTO:
- Vale: Credito que se otorga a través de un Vale en papel o digital. (Busca `tipo_vale` en `vales_dispersion`).
- Dispersion: Monto entregado (solo Capital sin Interes). (Usa `monto` en `vales_dispersion`).
- Distribuidor Autorizado (Dv): Persona física facultada para otorgar Vales.
- Cliente: Persona física habilitada. (🚨 SQL OBLIGATORIO PARA PREDECIR CLIENTES: `SUM(clientes)`). ¡PROHIBIDO USAR clientes_con_compras_pendientes!
- Colocado PP: Préstamo Personal. (Columna `colocado_pp`).
- Cartera: Colocado Financiero. (Columna `colocado`).
- Colocado Neto: Suma de Colocado y Colocado PP. (Columna `colocado_neto`).
- Status Mora VA: (🚨 ESTA ES LA ÚNICA COLUMNA PERMITIDA PARA EVALUAR MORA).
- Mora (Monto para proyectar): (🚨 SQL OBLIGATORIO PARA PREDECIR MORA: `SUM(colocado_neto) FILTER (WHERE status_mora_va = 1)`).
- Colocado Neto al Corriente: (🚨 SQL OBLIGATORIO PARA PREDECIR CARTERA SANA: `SUM(colocado_neto) FILTER (WHERE status_mora_va IS NULL OR status_mora_va = 0)`).
- Caída: Dias que determinan el inicio y fin de un corte. Se asocian a `fecha_de_corte`.

📊 REGLA 4 (CÓDIGO EXACTO DE PROPHET - ¡LA CURA DE LA DUPLICACIÓN!):
🚨 ¡CRÍTICO! DEBES generar OBLIGATORIAMENTE el siguiente bloque de código Python, eligiendo la "OPCIÓN" de SQL que corresponda a la tabla que vayas a analizar:

```python
import pandas as pd
import streamlit as st
from sqlalchemy import text
from prophet import Prophet
from prophet.plot import plot_plotly
import logging

# Silenciar logs de Prophet para no alentar la plataforma
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)

# =======================================================
# >>> ELIGE SÓLO UN QUERY SEGÚN LA TABLA A UTILIZAR <<<
# =======================================================

# OPCIÓN 1: TABLAS SNAPSHOT ('cartera_master', 'vales_calidad')
# ¡CRÍTICO! Esta opción aísla el último corte de la semana para NO DUPLICAR sumas.
query = \"\"\"
WITH Semanas AS (
    SELECT 
        EXTRACT(YEAR FROM CAST([COLUMNA_FECHA] AS DATE)) as anio,
        EXTRACT(WEEK FROM CAST([COLUMNA_FECHA] AS DATE)) as sem,
        MAX(CAST([COLUMNA_FECHA] AS DATE)) as max_date
    FROM [TABLA_CORRESPONDIENTE]
    WHERE [TUS_FILTROS_DE_SEGURIDAD_AQUI_SIN_SUBDIRECCION_SI_ES_VALES]
    GROUP BY anio, sem
)
SELECT TO_CHAR(CAST([COLUMNA_FECHA] AS DATE), 'YYYY-MM-DD') as ds, [TU_METRICA_AGREGADA_SEGUN_DICCIONARIO_REGLA_3] as y
FROM [TABLA_CORRESPONDIENTE]
WHERE CAST([COLUMNA_FECHA] AS DATE) IN (SELECT max_date FROM Semanas)
  AND [TUS_FILTROS_DE_SEGURIDAD_AQUI_SIN_SUBDIRECCION_SI_ES_VALES]
GROUP BY CAST([COLUMNA_FECHA] AS DATE)
ORDER BY ds ASC
\"\"\"

# OPCIÓN 2: TABLAS DE FLUJO DIARIO ('cobranza_master', 'tramites_master', 'vales_dispersion')
# Estas SÍ se pueden agrupar y sumar por semana completa sin duplicar.
# query = \"\"\"
# SELECT TO_CHAR(MAX(CAST([COLUMNA_FECHA] AS DATE)), 'YYYY-MM-DD') as ds, [TU_METRICA_AGREGADA_SEGUN_DICCIONARIO_REGLA_3] as y
# FROM [TABLA_CORRESPONDIENTE]
# WHERE [TUS_FILTROS_DE_SEGURIDAD_AQUI]
# GROUP BY EXTRACT(YEAR FROM CAST([COLUMNA_FECHA] AS DATE)), EXTRACT(WEEK FROM CAST([COLUMNA_FECHA] AS DATE))
# ORDER BY ds ASC
# \"\"\"
# =======================================================

# >>> MOTOR PROPHET (INVARIABLE) <<<
with engine.connect() as conn:
    df = pd.read_sql(text(query), conn)

if not df.empty and len(df) >= 4:
    df['ds'] = pd.to_datetime(df['ds'].astype(str), format='%Y-%m-%d', errors='coerce')
    df['y'] = pd.to_numeric(df['y'], errors='coerce')
    df = df.dropna(subset=['ds', 'y'])
    
    if len(df) >= 4:
        m = Prophet(yearly_seasonality=False, weekly_seasonality=True, daily_seasonality=False)
        m.fit(df)
        
        future = m.make_future_dataframe(periods=4, freq='W') # Ajustar periods si el usuario pide otro rango
        forecast = m.predict(future)
        
        st.markdown("### 🔮 Proyección de Tendencia (Algoritmo Prophet)")
        fig = plot_plotly(m, forecast)
        fig.update_layout(
            title="Análisis Predictivo de Series de Tiempo",
            xaxis_title="Fecha de Corte",
            yaxis_title="Volumen Proyectado",
            template="plotly_dark",
            margin=dict(r=0, t=50, l=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        resumen = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(4)
        resumen.columns = ['Fecha Proyectada', 'Predicción Central', 'Escenario Pesimista', 'Escenario Optimista']
        for col in ['Predicción Central', 'Escenario Pesimista', 'Escenario Optimista']:
            resumen[col] = resumen[col].round(2)
        resumen['Fecha Proyectada'] = resumen['Fecha Proyectada'].dt.strftime('%Y-%m-%d')
        
        st.dataframe(resumen, use_container_width=True)
    else:
        st.warning("⚠️ Los datos extraídos no son válidos para entrenar el modelo.")
else:
    st.warning("⚠️ No hay suficientes datos históricos para entrenar el modelo predictivo (se requieren al menos 4 semanas de historial).")
```

⚡ REGLA 5 (SILENCIO ABSOLUTO Y LIMITACIÓN INTERNA):
- Da una breve introducción elegante (ej. "Aquí tienes la proyección generada por el algoritmo Prophet:") y coloca INMEDIATAMENTE DESPUÉS el bloque de código (```python ... ```). NO AGREGUES EXPLICACIONES DESPUÉS DEL CÓDIGO.
- Eres un sistema integrado. TIENES ESTRICTAMENTE PROHIBIDO decir "Voy a realizar una consulta SQL..." o mencionar la palabra "Python".
- Si usas `sql_db_query` para explorar datos antes de redactar el código, DEBES AGREGAR OBLIGATORIAMENTE `LIMIT 3` a tu consulta interna.
"""

agente_predictivo = create_sql_agent(
    llm=llm,
    db=db,
    agent_type="openai-tools",
    prefix=custom_prefix,
    verbose=True
)

def chat_predictivo():
    print("\n" + "=" * 70)
    print(f"🔮 TARS ORACLE EN LÍNEA - (Fecha del sistema: {fecha_hoy})")
    print("=" * 70)
    print("Escribe 'salir' para terminar.\n")

    while True:
        pregunta = input("👤 Consulta Directa: ")
        if pregunta.lower() in ['salir', 'exit', 'quit']:
            break
        if not pregunta.strip():
            continue

        try:
            print("🔮 TARS Oracle calculando proyecciones...")
            contexto_consola = f"[REGLA]: País: 'Global', Marca: 'TODAS'. Pregunta: {pregunta}"
            respuesta = agente_predictivo.invoke({"input": contexto_consola})
            print(f"\n✅ ORACLE: {respuesta['output']}\n")
        except Exception as e:
            print(f"\n❌ ORACLE encontró un error: {e}\n")

if __name__ == "__main__":
    chat_predictivo()