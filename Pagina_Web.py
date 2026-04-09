import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import sys
import os
import time
import io
import base64
import hashlib
import uuid
import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Librería para extraer texto de PDFs en tiempo real
try:
    from pypdf import PdfReader
except ImportError:
    pass

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="TARS | Enterprise Intelligence", layout="wide", initial_sidebar_state="expanded")

# --- CONEXIÓN A BASE DE DATOS Y SEGURIDAD (AWS RDS) ---
load_dotenv()
try:
    engine = create_engine(f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}')
except Exception as e:
    st.error(f"Error Crítico: No hay conexión al motor de PostgreSQL: {e}")

# CREACIÓN AUTOMÁTICA DE TABLAS
def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS usuarios_tars (
                id SERIAL PRIMARY KEY, correo VARCHAR(150) UNIQUE NOT NULL,
                usuario VARCHAR(100) NOT NULL, password_hash VARCHAR(256) NOT NULL,
                pais VARCHAR(500) NOT NULL, marca VARCHAR(500) NOT NULL,
                rol VARCHAR(20) DEFAULT 'USER', aprobado BOOLEAN DEFAULT FALSE,
                estado VARCHAR(20) DEFAULT 'PENDIENTE'
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS historial_chat (
                id SERIAL PRIMARY KEY, correo_usuario VARCHAR(150) NOT NULL,
                chat_id VARCHAR(50) NOT NULL, titulo_chat VARCHAR(100) DEFAULT 'Nueva Consulta',
                rol_mensaje VARCHAR(20) NOT NULL, contenido TEXT NOT NULL,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
init_db()

# --- MARCAS DE VALES INTEGRADAS ---
MARCAS_POR_PAIS = {
    "Mexico": ["La Casita", "La Guerita", "La Lupita", "La Moderna", "Presico CD", "Presico MX", "Prestamos Unidos", "Rapi Vale", "Viva Vale", "Vale Amigo", "TODOS LOS VALES", "TODAS (Director)"],
    "Guatemala": ["Presico", "La Casita", "Pistiyo", "TODAS (Director)"],
    "Peru": ["Presico", "La Casita", "Vale Perú", "TODAS (Director)"],
    "El Salvador": ["Pistiyo", "La Moderna", "TODAS (Director)"],
    "Colombia": ["La Moderna", "TODAS (Director)"],
    "Honduras": ["Pistiyo", "TODAS (Director)"],
    "Nicaragua": ["Pistiyo", "TODAS (Director)"],
    "Global (Dueños)": ["ACCESO TOTAL"]
}

# --- LÓGICA DE BASE DE DATOS ---
def hash_password(password): return hashlib.sha256(str.encode(password)).hexdigest()

def crear_usuario(correo, usuario, password, pais_list, marca_list):
    pais_str = "|".join(pais_list)
    marca_str = "|".join(marca_list)
    try:
        with engine.begin() as conn:
            conn.execute(text(f"INSERT INTO usuarios_tars (correo, usuario, password_hash, pais, marca, estado) VALUES ('{correo}', '{usuario}', '{hash_password(password)}', '{pais_str}', '{marca_str}', 'PENDIENTE')"))
        return True
    except Exception: return False

def verificar_login(correo, password):
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT usuario, pais, marca, rol, estado, aprobado FROM usuarios_tars WHERE correo = '{correo}' AND password_hash = '{hash_password(password)}'")).fetchone()

def guardar_mensaje(correo, chat_id, titulo, rol, contenido):
    contenido_limpio = contenido.replace("'", "''")
    with engine.begin() as conn:
        conn.execute(text(f"INSERT INTO historial_chat (correo_usuario, chat_id, titulo_chat, rol_mensaje, contenido) VALUES ('{correo}', '{chat_id}', '{titulo}', '{rol}', '{contenido_limpio}')"))

def obtener_lista_chats(correo):
    with engine.connect() as conn:
        query = f"""
            SELECT chat_id, MAX(titulo_chat) as titulo, MIN(fecha) as fecha_creacion 
            FROM historial_chat 
            WHERE correo_usuario = '{correo}' 
            GROUP BY chat_id 
            ORDER BY fecha_creacion DESC
        """
        chats = conn.execute(text(query)).fetchall()
        return [{"chat_id": row[0], "titulo": row[1]} for row in chats]

def cargar_mensajes_chat(chat_id):
    with engine.connect() as conn:
        historial = conn.execute(text(f"SELECT rol_mensaje, contenido FROM historial_chat WHERE chat_id = '{chat_id}' ORDER BY id ASC")).fetchall()
        return [{"role": row[0], "content": row[1]} for row in historial]

def borrar_chat_especifico(chat_id):
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM historial_chat WHERE chat_id = '{chat_id}'"))

# ====================================================================
# 🚀 MOTOR DE KPIs MEJORADO (CACHÉ GLOBAL DE 1 HORA CON INDICADOR NATIVO)
# ====================================================================
# NOTA: Al no poner 'show_spinner=False', Streamlit mostrará su circulito pequeño arriba a la derecha
@st.cache_data(ttl=3600)
def obtener_kpis(pais_filtro, marca_filtro):
    filtro_pais = "1=1"
    # Aceptamos los nombres limpios de la interfaz
    if pais_filtro not in ["TODOS (Global)", "Global", "Todos los Países"]:
        if pais_filtro in ["Mexico", "México", "MEXICO", "MÉXICO"]:
            filtro_pais = "(TRIM(UPPER(pais)) = 'MEXICO' OR TRIM(UPPER(pais)) = 'MÉXICO')"
        elif pais_filtro in ["Peru", "Perú", "PERU", "PERÚ"]:
            filtro_pais = "(TRIM(UPPER(pais)) = 'PERU' OR TRIM(UPPER(pais)) = 'PERÚ')"
        else:
            filtro_pais = f"TRIM(UPPER(pais)) = UPPER('{pais_filtro}')"

    def armar_filtro_marca(col_marca):
        if marca_filtro in ["TODAS", "TODAS (Director)", "ACCESO TOTAL", "Todas las Marcas"]: return "1=1"
        if marca_filtro == "TODOS LOS VALES": return f"TRIM(UPPER({col_marca})) IN ('RAPI VALE', 'VIVA VALE', 'VALE PERÚ', 'VALE PERU')"
        if marca_filtro == "Vale Perú": return f"(TRIM(UPPER({col_marca})) = 'VALE PERÚ' OR TRIM(UPPER({col_marca})) = 'VALE PERU')"
        return f"TRIM(UPPER({col_marca})) = UPPER('{marca_filtro}')"

    filtro_base_cart_cob = f"{filtro_pais} AND {armar_filtro_marca('unidad_de_negocio')}"
    filtro_tramites_base = f"{filtro_pais} AND {armar_filtro_marca('marca')}"

    # 🚨 BLINDAJE ANTI-INSEGURIDAD GLOBAL 🚨
    filtro_rutas_inseguras = "ruta NOT IN (SELECT DISTINCT ruta FROM cartera_master WHERE subdireccion ILIKE '%Inseguridad%' AND ruta IS NOT NULL)"

    filtro_cartera = f"{filtro_base_cart_cob} AND (subdireccion NOT ILIKE '%Inseguridad%' OR subdireccion IS NULL)"
    filtro_cobranza = f"{filtro_base_cart_cob} AND (tipo_moneda = 'MXN' OR tipo_moneda IS NULL) AND {filtro_rutas_inseguras}"
    filtro_tramites = f"{filtro_tramites_base} AND {filtro_rutas_inseguras}"

    kpis = {
        "rutas": "0", "clientes_totales": "0", "clientes_corriente": "0", "faltas": "0", "ip": "0%",
        "cartera": "$0", "cartera_corriente": "$0", "cartera_atraso": "$0",
        "cuota_cobranza": "$0", "recuperacion": "$0", "tramites": "0", "monto_entregado": "$0", 
        "fecha_corte_cart": "N/A", "fecha_corte_cob": "N/A", "fecha_corte_tram": "N/A"
    }
    
    mensaje_error = None

    try:
        with engine.connect() as conn:
            fecha_max_cart = conn.execute(text(f"SELECT MAX(fecha) FROM cartera_master WHERE {filtro_cartera}")).scalar()
            if fecha_max_cart:
                fecha_str_cart = str(fecha_max_cart)[:10]
                kpis["fecha_corte_cart"] = fecha_str_cart
                
                query_cart = f"""
                    SELECT 
                        COUNT(DISTINCT ruta), 
                        SUM(CAST(clientes_totales AS NUMERIC)), 
                        SUM(CAST(clientes_al_corriente AS NUMERIC)), 
                        SUM(CAST(faltas AS NUMERIC)), 
                        SUM(CAST(cartera_total AS NUMERIC)), 
                        SUM(COALESCE(CAST(NULLIF(NULLIF(TRIM(CAST(cartera_sin_atrasos AS TEXT)), '-'), '') AS NUMERIC), 0))
                    FROM cartera_master 
                    WHERE CAST(fecha AS DATE) = '{fecha_str_cart}' AND {filtro_cartera}
                """
                res_cart = conn.execute(text(query_cart)).fetchone()
                
                if res_cart:
                    kpis["rutas"] = f"{int(res_cart[0] or 0):,}"
                    cli_tot, cli_corr = int(res_cart[1] or 0), int(res_cart[2] or 0)
                    kpis["clientes_totales"] = f"{cli_tot:,}"; kpis["clientes_corriente"] = f"{cli_corr:,}"; kpis["faltas"] = f"{int(res_cart[3] or 0):,}"
                    cart_tot, cart_sin_atraso = float(res_cart[4] or 0), float(res_cart[5] or 0)
                    kpis["cartera"] = f"${cart_tot:,.2f}"; kpis["cartera_corriente"] = f"${cart_sin_atraso:,.2f}"; kpis["cartera_atraso"] = f"${cart_tot - cart_sin_atraso:,.2f}"
                    if cli_tot > 0: kpis["ip"] = f"{(cli_corr / cli_tot) * 100:.1f}%"

            fecha_max_cob = conn.execute(text(f"SELECT MAX(fecha_corte) FROM cobranza_master WHERE {filtro_cobranza}")).scalar()
            if fecha_max_cob:
                if isinstance(fecha_max_cob, str): dt_cob = datetime.datetime.strptime(str(fecha_max_cob)[:10], '%Y-%m-%d').date()
                else: dt_cob = fecha_max_cob.date() if hasattr(fecha_max_cob, 'date') else fecha_max_cob
                inicio_sem_cob = dt_cob - datetime.timedelta(days=dt_cob.weekday()) 
                fin_sem_cob = inicio_sem_cob + datetime.timedelta(days=6)
                kpis["fecha_corte_cob"] = f"Del {inicio_sem_cob.strftime('%d/%m/%Y')} al {fin_sem_cob.strftime('%d/%m/%Y')}"
                
                query_cob = f"""
                    SELECT 
                        SUM(CAST(cuota_cobranza_del_dia AS NUMERIC)), 
                        SUM(CAST(pago_cobranza_del_dia AS NUMERIC)) 
                    FROM cobranza_master 
                    WHERE CAST(fecha_corte AS DATE) >= '{inicio_sem_cob}' 
                    AND CAST(fecha_corte AS DATE) <= '{fin_sem_cob}' 
                    AND {filtro_cobranza}
                """
                res_cob = conn.execute(text(query_cob)).fetchone()
                if res_cob: kpis["cuota_cobranza"] = f"${float(res_cob[0] or 0):,.2f}"; kpis["recuperacion"] = f"${float(res_cob[1] or 0):,.2f}"

            try:
                fecha_max_tram = conn.execute(text(f"SELECT MAX(fecha_desembolso) FROM tramites_master WHERE {filtro_tramites}")).scalar()
            except:
                filtro_tramites = filtro_tramites_base
                fecha_max_tram = conn.execute(text(f"SELECT MAX(fecha_desembolso) FROM tramites_master WHERE {filtro_tramites}")).scalar()

            if fecha_max_tram:
                if isinstance(fecha_max_tram, str): dt_tram = datetime.datetime.strptime(str(fecha_max_tram)[:10], '%Y-%m-%d').date()
                else: dt_tram = fecha_max_tram.date() if hasattr(fecha_max_tram, 'date') else fecha_max_tram
                inicio_sem_tram = dt_tram - datetime.timedelta(days=dt_tram.weekday())
                fin_sem_tram = inicio_sem_tram + datetime.timedelta(days=6)
                kpis["fecha_corte_tram"] = f"Del {inicio_sem_tram.strftime('%d/%m/%Y')} al {fin_sem_tram.strftime('%d/%m/%Y')}"
                
                query_tramites = f"""
                    SELECT COUNT(DISTINCT id_desembolso), SUM(CAST(capital AS NUMERIC)) 
                    FROM tramites_master 
                    WHERE CAST(fecha_desembolso AS DATE) >= '{inicio_sem_tram}' 
                    AND CAST(fecha_desembolso AS DATE) <= '{fin_sem_tram}' 
                    AND {filtro_tramites}
                """
                res_tram = conn.execute(text(query_tramites)).fetchone()
                if res_tram: kpis["tramites"] = f"{int(res_tram[0] or 0):,}"; kpis["monto_entregado"] = f"${float(res_tram[1] or 0):,.2f}"

    except Exception as e: mensaje_error = str(e)
    return kpis, mensaje_error

# --- FUNCIONES VISUALES Y LOTTIES ---
def load_image_base64(path):
    try:
        with open(path, "rb") as image_file: return base64.b64encode(image_file.read()).decode()
    except Exception: return ""
path_logo_empresa = r"C:\Users\EQUIPO.LAPTOP-44TK0PHA\Documents\TARS\assets\image_1cdc41.png"

def lottie_success():
    return """<script src="https://unpkg.com/@lottiefiles/dotlottie-wc@0.9.3/dist/dotlottie-wc.js" type="module"></script><div style="display:flex; justify-content:center; align-items:center;"><dotlottie-wc src="https://lottie.host/e13b8e6c-faec-4bdb-adfd-40eba4491a88/4oGH8sdn6W.lottie" autoplay style="width:80px; height:80px;"></dotlottie-wc></div>"""

def lottie_thinking_cube():
    return """<script src="https://unpkg.com/@lottiefiles/dotlottie-wc@0.9.3/dist/dotlottie-wc.js" type="module"></script><div style="display:flex; justify-content:flex-start; align-items:center; background-color: rgba(49, 130, 206, 0.1); padding: 10px; border-radius: 10px; border: 1px solid #3182ce;"><dotlottie-wc src="https://lottie.host/26dec926-8037-4911-a869-e16d56fe39ca/oSldk3HFRZ.lottie" autoplay loop style="width:60px; height:60px;"></dotlottie-wc><span style="margin-left: 15px; color:#3182ce; font-weight:600; font-size: 1.1em; animation: pulse 1.5s infinite;">TARS analizando y calculando...</span></div>"""

def lottie_robot_hello():
    return """
    <script src="https://unpkg.com/@lottiefiles/dotlottie-wc@0.9.8/dist/dotlottie-wc.js" type="module"></script>
    <div style="display:flex; justify-content:center; align-items:center;">
        <dotlottie-wc src="https://lottie.host/18b67a8d-98e6-4378-bbb9-6d2d27db84db/rKyFYqkjDq.lottie" style="width: 280px; height: 280px;" autoplay loop></dotlottie-wc>
    </div>
    """

def lottie_theme_interactive():
    return """
    <script src="https://unpkg.com/@lottiefiles/dotlottie-wc@0.9.3/dist/dotlottie-wc.js" type="module"></script>
    <div style="display:flex; justify-content:center; cursor:pointer;" onclick="ejecutarCambioPython()">
        <dotlottie-wc src="https://lottie.host/54ac1bed-66da-49c5-a68f-2f802eec6399/Y3DWEdYCfe.lottie" stateMachineId="StateMachine1" style="width:110px; height:110px;"></dotlottie-wc>
    </div>
    <script>
        function ejecutarCambioPython() {
            const botones = window.parent.document.querySelectorAll('button');
            botones.forEach(btn => {
                if(btn.innerText.includes('HIDDEN_THEME_BTN')) { btn.click(); }
            });
        }
        function cazarFantasma() {
            const botones = window.parent.document.querySelectorAll('button');
            botones.forEach(btn => {
                if(btn.innerText.includes('HIDDEN_THEME_BTN')) {
                    let contenedor = btn.closest('div[data-testid="stElementContainer"]');
                    if(contenedor) { contenedor.style.display = 'none'; }
                }
            });
        }
        cazarFantasma();
        setInterval(cazarFantasma, 200);
    </script>
    """

# --- MANEJO DE ESTADOS GLOBALES ---
if "loaded" not in st.session_state: st.session_state.loaded = False
if "logged_in" not in st.session_state: st.session_state.logged_in = False
if "user_info" not in st.session_state: st.session_state.user_info = None
if "correo_actual" not in st.session_state: st.session_state.correo_actual = None
if "theme" not in st.session_state: st.session_state.theme = "dark"
if "chat_id_actual" not in st.session_state: st.session_state.chat_id_actual = str(uuid.uuid4())
if "titulo_chat_actual" not in st.session_state: st.session_state.titulo_chat_actual = "Nueva Consulta"
if "modelo_seleccionado" not in st.session_state: st.session_state.modelo_seleccionado = "GPT-4o (Máxima Inteligencia)"
if "uploader_key" not in st.session_state: st.session_state.uploader_key = 0

# --- DISEÑO DINÁMICO EXTREMO ---
if st.session_state.theme == "dark":
    bg_color, text_color, panel_color, border_color, accent_color = "#080c14", "#e2e8f0", "#121826", "#1e293b", "#3182ce"
    table_hover = "#1e293b"
    background_css = f"""
        background-color: {bg_color} !important;
        background-image: linear-gradient(rgba(49, 130, 206, 0.05) 1px, transparent 1px), linear-gradient(90deg, rgba(49, 130, 206, 0.05) 1px, transparent 1px) !important;
        background-size: 30px 30px !important;
    """
else:
    bg_color, text_color, panel_color, border_color, accent_color = "#f4f7fc", "#0f172a", "#ffffff", "#cbd5e1", "#2563eb"
    table_hover = "#e2e8f0"
    background_css = f"background-color: {bg_color} !important;"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;800&display=swap');

html, body, [class*="css"], .stApp, .stApp > header {{ font-family: 'Inter', sans-serif; {background_css} color: {text_color} !important; transition: all 0.3s ease; }}
p, h1, h2, h3, h4, h5, h6, span, label, div:not(.tars-title) {{ color: {text_color} !important; }}

.stTextInput>div>div>input {{ border-radius: 6px; background-color: {panel_color} !important; color: {text_color} !important; border: 1px solid {border_color}; transition: all 0.3s ease; }}
.stTextInput>div>div>input:focus {{ border-color: {accent_color}; box-shadow: 0 0 5px {accent_color}; }}
.stSelectbox>div>div>div, .stMultiSelect>div>div>div {{ background-color: {panel_color} !important; border: 1px solid {border_color}; color: {text_color} !important; border-radius: 6px; transition: all 0.3s ease; }}

.stButton>button {{ border-radius: 6px; font-weight: 600; transition: all 0.3s ease; background-color: {panel_color} !important; border: 1px solid {border_color};}}
.stButton>button * {{ color: {text_color} !important; }}
.stButton>button:hover {{ border-color: {accent_color} !important; transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
.stButton>button:hover * {{ color: {accent_color} !important; }}

.btn-primary>button {{ background-color: {accent_color} !important; border: none !important; text-transform: uppercase; letter-spacing: 0.5px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
.btn-primary>button * {{ color: white !important; }}
.btn-primary>button:hover {{ filter: brightness(1.2); transform: translateY(-2px); box-shadow: 0 6px 12px rgba(0,0,0, 0.2); }}

.btn-danger>button {{ background-color: #e53e3e !important; color: white !important; border: none !important; }}
.btn-danger>button:hover {{ filter: brightness(1.2); transform: scale(1.02); }}

[data-testid="collapsedControl"] {{ display: flex !important; visibility: visible !important; opacity: 1 !important; z-index: 999999 !important; background-color: {panel_color} !important; border-radius: 8px !important; border: 1px solid {border_color} !important; padding: 5px !important; }}
[data-testid="collapsedControl"] svg {{ fill: {accent_color} !important; color: {accent_color} !important; }}

[data-testid="stToolbar"] {{ visibility: hidden !important; display: none !important; }}
[data-testid="stHeader"] {{ background: transparent !important; height: 0px !important; }}
#MainMenu {{visibility: hidden;}} footer {{visibility: hidden;}}

.stChatMessage.user {{ background-color: {panel_color} !important; border-radius: 12px 12px 0px 12px; border: 1px solid {border_color}; transition: all 0.3s ease; }}
.stChatMessage.user * {{ color: {text_color} !important; }}
.stChatMessage.assistant {{ background-color: {accent_color} !important; border-radius: 12px 12px 12px 0px; box-shadow: 0 4px 15px rgba(0,0,0,0.2); transition: all 0.3s ease; }}
.stChatMessage.assistant * {{ color: white !important; }}

.tars-title {{ font-size: 4.5rem; font-weight: 800; text-align: center; background: linear-gradient(90deg, #3182ce, #63b3ed, #3182ce); background-size: 200% auto; color: transparent !important; -webkit-background-clip: text; animation: gradientAnim 3s linear infinite; margin-bottom: -15px; padding-top: 10px; display: inline-block; width: 100%; }}
@keyframes gradientAnim {{ to {{ background-position: 200% center; }} }}

[data-testid="stSidebar"] {{ background-color: {panel_color} !important; border-right: 1px solid {border_color}; padding-top: 20px; transition: background-color 0.3s ease; }}
.radio-modelos > div {{ background-color: {panel_color} !important; padding: 10px; border-radius: 8px; border: 1px solid {border_color}; }}
.menu-letters {{ text-align: center; font-weight: 800; font-size: 1.5rem; letter-spacing: 6px; color: {accent_color} !important; text-transform: uppercase; margin-bottom: 5px; }}
.menu-line {{ height: 2px; width: 40px; background-color: {accent_color}; margin: 0 auto 20px auto; border-radius: 2px; }}

.tars-table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; font-family: 'Inter', sans-serif; background-color: {panel_color}; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); margin-top: 20px; margin-bottom: 20px; }}
.tars-table thead tr {{ background-color: {accent_color}; color: white; text-align: left; font-weight: 600; }}
.tars-table th, .tars-table td {{ padding: 12px 15px; border-bottom: 1px solid {border_color}; color: {text_color}; }}
.tars-table tbody tr:nth-of-type(even) {{ background-color: rgba(0,0,0,0.02); }}
.tars-table tbody tr:hover {{ background-color: {table_hover}; cursor: pointer; }}

[data-testid="stMetricValue"] {{ font-size: 1.6rem !important; color: {accent_color} !important; font-weight: 800; transition: color 0.3s ease; }}
[data-testid="stMetricLabel"] {{ font-size: 0.75rem !important; color: gray !important; text-transform: uppercase; letter-spacing: 0.5px; }}
</style>
""", unsafe_allow_html=True)

def render_header():
    base64_logo_empresa = load_image_base64(path_logo_empresa)
    img_html = f"<img src='data:image/png;base64,{base64_logo_empresa}' style='height: 35px; opacity: 1;'/>" if base64_logo_empresa else ""
    header_html = f"<div style='display:flex; align-items:center; justify-content:space-between; padding:10px 30px; border-bottom:1px solid {border_color}; background-color:{panel_color}; margin-bottom:20px; padding-top: 40px;'><div style='display:flex; align-items:center;'>{img_html}<span style='color:{text_color} !important; font-weight:600; font-size:1.2em; margin-left:15px;'>TARS Platform</span></div><div style='text-align:right;'><span style='color:{accent_color} !important; font-weight:600; font-size:0.8em; text-transform:uppercase; letter-spacing:1px;'>{st.session_state.modelo_seleccionado.split(' ')[0]} Engine</span></div></div>"
    st.markdown(header_html, unsafe_allow_html=True)

render_header()

# --- CARGA MEMORIZADA DE AGENTES ---
@st.cache_resource
def iniciar_agentes():
    path_actual = os.path.dirname(os.path.abspath(__file__))
    if path_actual not in sys.path: sys.path.append(path_actual)
    path_padre = os.path.dirname(path_actual)
    if path_padre not in sys.path: sys.path.append(path_padre)
    
    agente_s, agente_p, err = None, None, "No inicializado."
    try: 
        from Modulo_IA.Agente_SQL import agente_tars
        agente_s = agente_tars
    except Exception as e1:
        try:
            from Agente_SQL import agente_tars
            agente_s = agente_tars
        except Exception as e2:
            err = f"Error SQL: {e1} | {e2}"
        
    try:
        from Modulo_IA.Agente_PDF import agente_pdf
        agente_p = agente_pdf
    except Exception as e1:
        try:
            from Agente_PDF import agente_pdf
            agente_p = agente_pdf
        except Exception as e2:
            pass
        
    return agente_s, agente_p, err

agente_tars, agente_pdf, error_agente = iniciar_agentes()
sql_disponible = True if agente_tars else False
pdf_disponible = True if agente_pdf else False


if st.session_state.logged_in:
    with st.sidebar:
        st.markdown("<div class='menu-letters'>MENÚ</div><div class='menu-line'></div>", unsafe_allow_html=True)
        
        st.markdown(f"<div class='btn-primary'>", unsafe_allow_html=True)
        if st.button("➕ NUEVO CHAT", use_container_width=True):
            st.session_state.chat_id_actual = str(uuid.uuid4())
            st.session_state.titulo_chat_actual = "Nueva Consulta"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        
        st.divider()
        
        st.markdown(f"<p style='color:{text_color} !important; font-weight:600; margin-bottom:5px;'>⚙️ Motor de Procesamiento</p>", unsafe_allow_html=True)
        st.markdown("<div class='radio-modelos'>", unsafe_allow_html=True)
        st.session_state.modelo_seleccionado = st.radio(
            label="Selecciona la IA:",
            options=["GPT-4o (Máxima Inteligencia)", "GPT-4o-mini (Súper Rápido)"],
            label_visibility="collapsed"
        )
        st.markdown("</div>", unsafe_allow_html=True)

        st.divider()
        
        st.markdown(f"<p style='color:gray !important; font-size:0.8em; font-weight:600; margin-bottom:5px;'>TU HISTORIAL</p>", unsafe_allow_html=True)
        lista_chats = obtener_lista_chats(st.session_state.correo_actual)
        if not lista_chats:
            st.markdown(f"<p style='color:gray !important; font-size:0.8em;'>No hay chats recientes.</p>", unsafe_allow_html=True)
        else:
            for chat in lista_chats[:15]:
                col1, col2 = st.columns([4, 1])
                with col1:
                    prefijo = "🟢 " if chat['chat_id'] == st.session_state.chat_id_actual else "💬 "
                    if st.button(f"{prefijo}{chat['titulo'][:15]}...", key=f"load_{chat['chat_id']}", use_container_width=True):
                        st.session_state.chat_id_actual = chat['chat_id']
                        st.session_state.titulo_chat_actual = chat['titulo']
                        st.rerun()
                with col2:
                    if st.button("🗑️", key=f"del_{chat['chat_id']}"):
                        st.session_state[f"confirm_del_{chat['chat_id']}"] = True

                # CONFIRMACIÓN DE BORRADO SEGURO
                if st.session_state.get(f"confirm_del_{chat['chat_id']}", False):
                    st.warning("¿Seguro que deseas borrarlo?")
                    cx, cy = st.columns(2)
                    st.markdown("<div class='btn-danger'>", unsafe_allow_html=True)
                    if cx.button("Sí", key=f"y_{chat['chat_id']}", use_container_width=True):
                        borrar_chat_especifico(chat['chat_id'])
                        if st.session_state.chat_id_actual == chat['chat_id']:
                            st.session_state.chat_id_actual = str(uuid.uuid4())
                            st.session_state.titulo_chat_actual = "Nueva Consulta"
                        del st.session_state[f"confirm_del_{chat['chat_id']}"]
                        st.rerun()
                    st.markdown("</div>", unsafe_allow_html=True)
                    if cy.button("No", key=f"n_{chat['chat_id']}", use_container_width=True):
                        del st.session_state[f"confirm_del_{chat['chat_id']}"]
                        st.rerun()

        st.divider()
        
        st.markdown(f"<p style='color:gray !important; font-size:0.8em; font-weight:600; margin-bottom:5px;'>AYUDA Y SOPORTE</p>", unsafe_allow_html=True)
        st.info("Por favor acudir a **PLANEACIÓN Y ANÁLISIS DE DATOS**.\n\n📧 **Contacto:**\n- data.analitycs@caprepa.com\n- data.analitycs.1@caprepa.com")
        
        st.divider()
        
        st.markdown(f"""<div style='padding:10px; background-color:{bg_color}; border-radius:8px; border:1px solid {border_color}; margin-bottom:10px;'><p style='margin:0; font-size:0.7em; color:gray !important;'>PERFIL ACTIVO</p><p style='margin:0; font-weight:600; font-size:0.9em; color:{text_color} !important;'>{st.session_state.user_info['usuario']}</p></div>""", unsafe_allow_html=True)
        
        components.html(lottie_theme_interactive(), height=125)
            
        if st.button("🚪 Cerrar Sesión", use_container_width=True):
            st.session_state.logged_in = False
            st.rerun()

        if st.button("HIDDEN_THEME_BTN"):
            st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"
            st.rerun()

if not st.session_state.logged_in:
    col_espacio, col_form, col_espacio2 = st.columns([1, 1.2, 1])
    with col_form:
        components.html("""
        <script src="https://unpkg.com/@lottiefiles/dotlottie-wc@0.9.3/dist/dotlottie-wc.js" type="module"></script>
        <div style="display:flex; justify-content:center; align-items:center;">
            <dotlottie-wc src="https://lottie.host/26dec926-8037-4911-a869-e16d56fe39ca/oSldk3HFRZ.lottie" style="width: 250px; height: 250px;" autoplay loop></dotlottie-wc>
        </div>
        """, height=260)

        st.markdown("<div class='tars-title'>TARS</div><p style='text-align:center; color:gray !important; font-size:1.1rem; letter-spacing:3px; margin-bottom:30px;'>ENTERPRISE INTELLIGENCE</p>", unsafe_allow_html=True)
        tab_login, tab_registro = st.tabs(["Autenticación", "Solicitar Acceso"])
        
        with tab_login:
            with st.form("form_login"):
                correo_login = st.text_input("Correo Institucional")
                pass_login = st.text_input("Contraseña", type="password")
                st.markdown("<div class='btn-primary'>", unsafe_allow_html=True)
                submit_login = st.form_submit_button("Ingresar a Plataforma", use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
                
                if submit_login:
                    if correo_login == "admin" and pass_login == "admin":
                        st.session_state.logged_in = True
                        st.session_state.correo_actual = "admin"
                        st.session_state.user_info = {"usuario": "Administrador", "pais": "Global", "marca": "TODAS", "rol": "ADMIN"}
                        st.rerun()
                        
                    user_data = verificar_login(correo_login, pass_login)
                    if user_data:
                        estado_actual = user_data[4]
                        aprobado_boolean = user_data[5]
                        if estado_actual == 'APROBADO' or aprobado_boolean == True:
                            st.session_state.logged_in = True
                            st.session_state.correo_actual = correo_login
                            st.session_state.user_info = {"usuario": user_data[0], "pais": user_data[1], "marca": user_data[2], "rol": user_data[3]}
                            st.rerun()
                        elif estado_actual == 'RECHAZADO':
                            st.error("🚫 ACCESO DENEGADO: Tu solicitud ha sido rechazada.")
                        else: 
                            st.warning("⏳ CUENTA EN VALIDACIÓN: Tu perfil está pendiente de revisión por un Administrador.")
                    else: st.error("❌ Credenciales inválidas.")

        with tab_registro:
            reg_correo = st.text_input("Correo Institucional (Nuevo)")
            reg_usuario = st.text_input("Nombre Completo")
            reg_pass = st.text_input("Crea una Contraseña", type="password")
            
            paises_disponibles = list(MARCAS_POR_PAIS.keys())
            reg_pais = st.multiselect("País de Operación (Puedes elegir varios)", paises_disponibles)
            
            marcas_disponibles = []
            for p in reg_pais:
                marcas_disponibles.extend(MARCAS_POR_PAIS[p])
            marcas_disponibles = list(set(marcas_disponibles)) # Quitar duplicados
            
            reg_marca = st.multiselect("Marca Asignada (Puedes elegir varias)", marcas_disponibles)
            
            st.markdown("<div class='btn-primary'>", unsafe_allow_html=True)
            if st.button("Registrar Perfil", use_container_width=True):
                if reg_correo and reg_usuario and reg_pass and reg_pais and reg_marca:
                    if crear_usuario(reg_correo, reg_usuario, reg_pass, reg_pais, reg_marca):
                        components.html(lottie_success(), height=90)
                        st.success("✅ Perfil registrado exitosamente. El estado es PENDIENTE.")
                    else: st.error("❌ El correo ya está registrado.")
                else: st.warning("⚠️ Llena todos los campos y selecciona al menos un país y marca.")
            st.markdown("</div>", unsafe_allow_html=True)

else:
    if st.session_state.user_info["rol"] == "ADMIN":
        st.markdown(f"<h2 style='color: {text_color} !important;'>🛡️ Panel de Control Global</h2>", unsafe_allow_html=True)
        
        with engine.connect() as conn:
            usuarios_df = pd.read_sql("SELECT id, correo, usuario, pais, marca, rol, estado FROM usuarios_tars ORDER BY id ASC", conn)
            chats_count = pd.read_sql("SELECT correo_usuario, COUNT(DISTINCT chat_id) as total_chats FROM historial_chat GROUP BY correo_usuario", conn)
            
        usuarios_df = pd.merge(usuarios_df, chats_count, left_on="correo", right_on="correo_usuario", how="left")
        usuarios_df['total_chats'] = usuarios_df['total_chats'].fillna(0).astype(int)
        
        usuarios_df = usuarios_df.drop_duplicates(subset=['correo'])
        if 'correo_usuario' in usuarios_df.columns:
            usuarios_df.drop(columns=['correo_usuario'], inplace=True)
            
        st.markdown(usuarios_df.to_html(classes='tars-table', index=False), unsafe_allow_html=True)
        
        st.divider()
        
        col_acc, col_perm, col_hist = st.columns([1, 1.2, 1])
        with col_acc:
            st.markdown(f"<h4 style='color: {text_color} !important;'>🔐 Seguridad y Acceso</h4>", unsafe_allow_html=True)
            
            correo_accion = st.selectbox("Selecciona Usuario a modificar:", usuarios_df['correo'])
            accion_ejecutar = st.selectbox("Acción a Ejecutar", ["APROBAR ACCESO", "RECHAZAR / BLOQUEAR", "ELIMINAR DEFINITIVAMENTE"])
            
            st.markdown("<div class='btn-primary'>", unsafe_allow_html=True)
            if st.button("Ejecutar Estado", use_container_width=True):
                with engine.begin() as conn:
                    if accion_ejecutar == "ELIMINAR DEFINITIVAMENTE":
                        conn.execute(text(f"DELETE FROM usuarios_tars WHERE correo = '{correo_accion}'"))
                        msg_exito = "Usuario eliminado permanentemente."
                    elif accion_ejecutar == "RECHAZAR / BLOQUEAR":
                        conn.execute(text(f"UPDATE usuarios_tars SET estado = 'RECHAZADO', aprobado = FALSE WHERE correo = '{correo_accion}'"))
                        msg_exito = "Usuario bloqueado exitosamente."
                    else:
                        conn.execute(text(f"UPDATE usuarios_tars SET estado = 'APROBADO', aprobado = TRUE WHERE correo = '{correo_accion}'"))
                        msg_exito = "Usuario aprobado exitosamente."
                st.success(msg_exito)
                time.sleep(1)
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("<br><h5 style='color:gray;'>Restablecer Contraseña</h5>", unsafe_allow_html=True)
            
            st.info(f"🔑 Cambiando contraseña de: **{correo_accion}**")
            
            nueva_pass = st.text_input("Escribe la nueva contraseña", type="password")
            if st.button("Cambiar Contraseña", use_container_width=True):
                if nueva_pass:
                    with engine.begin() as conn:
                        conn.execute(text(f"UPDATE usuarios_tars SET password_hash = '{hash_password(nueva_pass)}' WHERE correo = '{correo_accion}'"))
                    st.success("Contraseña actualizada exitosamente.")
                else:
                    st.warning("Escribe una contraseña válida.")

        with col_perm:
            st.markdown(f"<h4 style='color: {text_color} !important;'>🌍 Editar Permisos (Multi-Marca)</h4>", unsafe_allow_html=True)
            st.info("Permite asignar varios países y marcas a un usuario a la vez.")
            correo_permiso = st.selectbox("Usuario a Editar Permisos:", usuarios_df['correo'], key="c_perm")
            
            paises_todos = list(MARCAS_POR_PAIS.keys())
            nuevos_paises = st.multiselect("Agregar Países Permitidos:", paises_todos)
            
            marcas_todas = []
            for p in nuevos_paises:
                marcas_todas.extend(MARCAS_POR_PAIS[p])
            
            nuevos_marcas = st.multiselect("Agregar Marcas Permitidas:", list(set(marcas_todas)))
            
            st.markdown("<div class='btn-primary'>", unsafe_allow_html=True)
            if st.button("Actualizar Permisos Globales", use_container_width=True):
                if nuevos_paises and nuevos_marcas:
                    str_p = "|".join(nuevos_paises)
                    str_m = "|".join(nuevos_marcas)
                    with engine.begin() as conn:
                        conn.execute(text(f"UPDATE usuarios_tars SET pais = '{str_p}', marca = '{str_m}' WHERE correo = '{correo_permiso}'"))
                    st.success("Permisos globales actualizados.")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.warning("Selecciona al menos un país y una marca.")
            st.markdown("</div>", unsafe_allow_html=True)

        with col_hist:
            st.markdown(f"<h4 style='color: {text_color} !important;'>🕵️‍♂️ Auditoría de Chats</h4>", unsafe_allow_html=True)
            usuario_auditar = st.selectbox("Usuario a auditar:", [""] + list(usuarios_df['correo']), key="aud_u")
            
            if usuario_auditar:
                with engine.connect() as conn:
                    historial_lista = pd.read_sql(f"SELECT DISTINCT chat_id, titulo_chat, MIN(fecha) as fecha FROM historial_chat WHERE correo_usuario = '{usuario_auditar}' GROUP BY chat_id, titulo_chat ORDER BY fecha DESC", conn)
                
                if not historial_lista.empty:
                    chat_a_revisar = st.selectbox("Consulta a leer:", historial_lista['titulo_chat'])
                    if chat_a_revisar:
                        chat_id_auditoria = historial_lista[historial_lista['titulo_chat'] == chat_a_revisar]['chat_id'].iloc[0]
                        mensajes_auditoria = cargar_mensajes_chat(chat_id_auditoria)
                        
                        with st.container(height=300):
                            for msg in mensajes_auditoria:
                                if msg['role'] == 'user':
                                    st.markdown(f"**👤 USUARIO:** {msg['content']}")
                                else:
                                    st.markdown(f"**🤖 TARS:** {msg['content']}")
                else:
                    st.info("Sin consultas registradas.")

    else:
        # --- LÓGICA DE INTERFAZ ELEGANTE Y NOMBRES PROFESIONALES ---
        lista_paises_usuario = [p.strip() for p in st.session_state.user_info['pais'].split('|')]
        lista_marcas_usuario = [m.strip() for m in st.session_state.user_info['marca'].split('|')]
        
        st.markdown(f"<h3 style='color: {text_color} !important; margin-bottom: 5px;'>🌐 Panel Operativo</h3>", unsafe_allow_html=True)
        
        # Identificamos si es un usuario global para mostrarle los nombres "bonitos"
        es_global = "Global (Dueños)" in lista_paises_usuario or "TODOS (Global)" in lista_paises_usuario
        
        if es_global:
            paises_disponibles = ["Todos los Países"] + [p for p in MARCAS_POR_PAIS.keys() if p != "Global (Dueños)"]
        else:
            paises_disponibles = lista_paises_usuario
            
        mostrar_dropdowns = len(paises_disponibles) > 1 or len(lista_marcas_usuario) > 1 or es_global
        
        if mostrar_dropdowns:
            col_kpi1, col_kpi2, col_kpi3 = st.columns([1, 1, 2])
            with col_kpi1:
                pais_seleccionado = st.selectbox("Selecciona un País:", paises_disponibles, label_visibility="collapsed")
                
            with col_kpi2:
                if pais_seleccionado == "Todos los Países":
                    marcas_disponibles = ["Todas las Marcas"]
                else:
                    if es_global or "TODAS (Director)" in lista_marcas_usuario or "ACCESO TOTAL" in lista_marcas_usuario:
                        marcas_disponibles = ["Todas las Marcas"] + [m for m in MARCAS_POR_PAIS.get(pais_seleccionado, []) if m not in ["TODAS (Director)", "TODAS", "ACCESO TOTAL"]]
                    else:
                        marcas_disponibles = [m for m in lista_marcas_usuario if m in MARCAS_POR_PAIS.get(pais_seleccionado, [])]
                
                # Para evitar que el menú se quede vacío si hay un cruce raro de permisos
                if not marcas_disponibles: marcas_disponibles = ["Todas las Marcas"]
                
                marca_seleccionada = st.selectbox("Selecciona una Marca:", marcas_disponibles, label_visibility="collapsed")
        else:
            pais_seleccionado = paises_disponibles[0]
            marca_seleccionada = lista_marcas_usuario[0]
            
        st.markdown(f"<h4 style='color:{accent_color} !important; margin-top: 10px;'>Resumen Operativo ({pais_seleccionado} | {marca_seleccionada})</h4>", unsafe_allow_html=True)
        
        # --- MENSAJE DE BIENVENIDA MÁGICO (SIN PANTALLA TENUE) ---
        nombre_corto = st.session_state.user_info['usuario'].split(' ')[0]
        carga_placeholder = st.empty()
        
        # Traducimos de vuelta los nombres bonitos para la consulta de base de datos
        pais_para_kpis = "Global" if pais_seleccionado == "Todos los Países" else pais_seleccionado
        marca_para_kpis = "TODAS" if marca_seleccionada == "Todas las Marcas" else marca_seleccionada
        
        with carga_placeholder.container():
            st.markdown(f"""
            <div style="padding: 15px; border-radius: 8px; background-color: rgba(49, 130, 206, 0.1); border: 1px solid #3182ce; margin-bottom: 20px;">
                <h4 style="color: #3182ce !important; margin-top: 0; font-weight: 600;">👋 ¡Bienvenido de vuelta, {nombre_corto}!</h4>
                <p style="margin-bottom: 8px; color: {text_color} !important;">Mientras AWS analiza tus métricas operativas, recuerda que puedes pedirme cosas como:</p>
                <ul style="margin-bottom: 0; color: {text_color} !important;">
                    <li>📊 <b>Gráficas:</b> <i>"Dibuja una gráfica de barras con el Top 5 de sucursales con mayor recuperación"</i></li>
                    <li>📥 <b>Reportes:</b> <i>"Genera un Excel descargable con las rutas en atraso"</i></li>
                    <li>📎 <b>Análisis:</b> <i>Sube un PDF en el chat y pregúntame sobre las reglas y políticas de la empresa.</i></li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
            
        # Ejecutamos la consulta. La ruedita girará arriba a la derecha.
        kpis, kpi_error = obtener_kpis(pais_para_kpis, marca_para_kpis)
        
        # Borramos la bienvenida cuando terminan de cargar los números
        carga_placeholder.empty()
            
        if kpi_error: st.error(f"🚨 ERROR EN LA BASE DE DATOS: {kpi_error}")
            
        st.caption(f"📅 Snapshot Cartera: **{kpis['fecha_corte_cart']}** | 📅 Última Semana Cobranza: **{kpis['fecha_corte_cob']}** | 📅 Trámites Reales: **{kpis['fecha_corte_tram']}**")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(label="Rutas", value=kpis['rutas'])
            st.metric(label="Cartera Total", value=kpis['cartera'])
            st.metric(label="Cuota Cobranza", value=kpis['cuota_cobranza'])
        with col2:
            st.metric(label="Clientes Totales", value=kpis['clientes_totales'])
            st.metric(label="Cartera Corriente", value=kpis['cartera_corriente'])
            st.metric(label="Recuperación", value=kpis['recuperacion'])
        with col3:
            st.metric(label="Clientes al Corriente", value=kpis['clientes_corriente'])
            st.metric(label="Cartera en Atraso", value=kpis['cartera_atraso'])
            st.metric(label="Trámites Reales", value=kpis['tramites'])
        with col4:
            st.metric(label="% IP (Al corriente)", value=kpis['ip'])
            st.metric(label="Faltas", value=kpis['faltas'])
            st.metric(label="Monto Entregado", value=kpis['monto_entregado'])

        st.divider()

        chat_placeholder = st.container()
        mensajes_db = cargar_mensajes_chat(st.session_state.chat_id_actual)
        
        with chat_placeholder:
            if not mensajes_db:
                components.html(lottie_robot_hello(), height=290)
                st.markdown(f"<h2 style='text-align:center; color:{text_color} !important; font-weight:600;'>¿En qué te puedo ayudar hoy, {nombre_corto}?</h2>", unsafe_allow_html=True)
                
                col_sug1, col_sug2, col_sug3 = st.columns(3)
                with col_sug1: st.info("📊 **Análisis SQL y Mapas:**\n\nCruza datos, saca métricas 'a la fecha' o pídeme ubicar clientes en el mapa.")
                with col_sug2: st.info("📎 **Documentos:**\n\nSube imágenes, Excel o PDF abajo para análisis profundo.")
                with col_sug3: st.info("🧠 **Políticas:**\n\nPregúntame sobre la Guía Completa, manuales y requisitos.")
                st.markdown("<br><br>", unsafe_allow_html=True)
                
            for msg in mensajes_db:
                with st.chat_message(msg["role"]): 
                    if "```python" in msg["content"] and "st." in msg["content"]:
                        partes = msg["content"].split("```python")
                        st.markdown(partes[0])
                        codigo_py = partes[1].split("```")[0].strip()
                        try:
                            exec(codigo_py)
                        except Exception as e:
                            st.warning(f"No se pudo renderizar visualización interactiva: {e}")
                    else:
                        st.markdown(msg["content"])

        with st.container():
            st.markdown(f"<p style='font-size:0.85em; color:gray !important; margin-bottom:0;'>📎 <b>Formatos Soportados:</b> Puedes adjuntar PNG, JPG, Excel, PDF o CSV (Máx 5).</p>", unsafe_allow_html=True)
            archivos_subidos = st.file_uploader("Adjuntar archivos", accept_multiple_files=True, type=['png', 'jpg', 'jpeg', 'pdf', 'xlsx', 'csv'], label_visibility="collapsed", key=f"uploader_{st.session_state.uploader_key}")
            
            if prompt := st.chat_input("Escribe tu solicitud analítica, pide 'el dato a la fecha' o genera un reporte en excel..."):
                if len(mensajes_db) == 0:
                    st.session_state.titulo_chat_actual = prompt[:30] + "..."
                
                st.session_state.uploader_key += 1
                texto_prompt = prompt
                es_pdf = False
                
                if archivos_subidos:
                    texto_prompt += "\n\n[El usuario adjuntó los siguientes archivos:]"
                    for archivo in archivos_subidos:
                        if archivo.name.endswith('.pdf'):
                            es_pdf = True
                            texto_prompt += f"\n\n--- Documento PDF: {archivo.name} ---"
                            try:
                                reader = PdfReader(archivo)
                                texto_extraido = "".join([page.extract_text() for page in reader.pages if page.extract_text()])
                                texto_prompt += f"\n[CONTENIDO DEL PDF]:\n{texto_extraido[:4000]}\n---"
                            except Exception as e:
                                texto_prompt += f"\n[Error al leer PDF: {e}]"
                        elif archivo.name.endswith('.csv'):
                            try:
                                df_temp = pd.read_csv(archivo)
                                texto_prompt += f"\n\n--- Archivo: {archivo.name} ---\n{df_temp.head(10).to_string()}"
                            except: pass
                        elif archivo.name.endswith('.xlsx'):
                            try:
                                df_temp = pd.read_excel(archivo)
                                texto_prompt += f"\n\n--- Archivo: {archivo.name} ---\n{df_temp.head(10).to_string()}"
                            except: pass

                guardar_mensaje(st.session_state.correo_actual, st.session_state.chat_id_actual, st.session_state.titulo_chat_actual, "user", texto_prompt)
                
                with chat_placeholder:
                    with st.chat_message("user"): st.markdown(prompt)
                    with st.chat_message("assistant"):
                        placeholder_anim = st.empty()
                        with placeholder_anim:
                            components.html(lottie_thinking_cube(), height=90)
                        
                        try:
                            # Aseguramos de mandar los nombres exactos a la BD para que no falle la IA
                            contexto_seguridad = f"[REGLA]: País: '{pais_para_kpis}', Marca: '{marca_para_kpis}'. Modelo: {st.session_state.modelo_seleccionado}. Pregunta: {texto_prompt}"
                            
                            palabras_negocio = ["manual", "política", "politica", "guía", "guia", "guía completa", "guia completa", "proceso", "requisito", "garantía", "garantia", "vale amigo", "présico", "presico", "regla", "documento"]
                            es_pregunta_negocio = any(palabra in prompt.lower() for palabra in palabras_negocio)

                            if (es_pdf or es_pregunta_negocio) and pdf_disponible:
                                respuesta = agente_pdf.invoke({"input": contexto_seguridad})
                            elif sql_disponible:
                                respuesta = agente_tars.invoke({"input": contexto_seguridad})
                            else:
                                respuesta = {'output': f"⚠️ El Agente SQL está fuera de línea.\n\n**Diagnóstico:** {error_agente}"}

                            placeholder_anim.empty()
                            guardar_mensaje(st.session_state.correo_actual, st.session_state.chat_id_actual, st.session_state.titulo_chat_actual, "assistant", respuesta['output'])
                            
                            if "```python" in respuesta['output'] and "st." in respuesta['output']:
                                partes = respuesta['output'].split("```python")
                                st.markdown(partes[0])
                                codigo_py = partes[1].split("```")[0].strip()
                                try:
                                    exec(codigo_py)
                                except Exception as e:
                                    st.warning(f"No se pudo renderizar visualización interactiva: {e}")
                            else:
                                st.markdown(respuesta['output'])
                                
                            time.sleep(0.1)
                            st.rerun()
                        except Exception as e:
                            placeholder_anim.empty()
                            st.error(f"Error en el procesamiento: {e}")
                            guardar_mensaje(st.session_state.correo_actual, st.session_state.chat_id_actual, st.session_state.titulo_chat_actual, "assistant", str(e))