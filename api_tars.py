import os
import uuid
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

# Importamos tu modelo de OpenAI que ya usas en TARS
from langchain_openai import ChatOpenAI

# 1. IMPORTAR TUS AGENTES ACTUALES
# (Asegúrate de que tus archivos actuales no tengan errores al importarse)
try:
    from Modulo_IA.Agente_PDF import agente_pdf
    from Modulo_IA.Agente_SQL import agente_tars
except ImportError:
    from Agente_PDF import agente_pdf
    from Agente_SQL import agente_tars

load_dotenv()

app = FastAPI(title="TARS Enterprise API")

# 2. CONFIGURACIÓN CORS (CRÍTICO PARA REACT)
# Esto permite que el ERP (React) se comunique con esta API sin bloqueos de seguridad
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # En producción, cambia "*" por la URL de tu ERP ("https://erp.caprepa...")
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3. MODELOS DE DATOS (Lo que el ERP nos va a enviar)
class ConsultaERP(BaseModel):
    pregunta: str
    correo_usuario: str
    pais: str
    marca: str
    token_sesion: str # Token para el Single Sign-On

# 4. EL ORQUESTADOR (El que decide si responde ahí o manda a Streamlit)
llm_router = ChatOpenAI(model="gpt-4o-mini", temperature=0)

@app.post("/api/chat_widget")
def procesar_chat_erp(consulta: ConsultaERP):
    try:
# Prompt de clasificación ajustado

        prompt_router = f"""
        Eres el clasificador de TARS. Evalúa esta pregunta del usuario: "{consulta.pregunta}"
        
        REGLA 1: Si es una pregunta sobre manuales, políticas, reglas de negocio o requisitos, responde exactamente: "TIPO_POLITICA".
        
        REGLA 2: Si pide expresamente gráficas, mapas interactivos, reportes en excel, proyecciones (Prophet), o tendencias de tiempo, responde exactamente: "TIPO_COMPLEJO".
        
        REGLA 3: Si pide UN DATO o KPI NUMÉRICO (cuántos clientes, cartera, recuperación, cuota, faltas), INCLUSO SI FILTRA POR PAÍS O MARCA (ej. "en Nicaragua", "de Presico"), responde exactamente: "TIPO_SQL_RAPIDO".
        """
        
        clasificacion = llm_router.invoke(prompt_router).content.strip()
        
        # --- RUTA 1: Pregunta de Políticas (Manuales PDF) ---
        if "TIPO_POLITICA" in clasificacion:
            contexto = f"[REGLA]: País: '{consulta.pais}', Marca: '{consulta.marca}'. Pregunta: {consulta.pregunta}"
            respuesta = agente_pdf.invoke({"input": contexto})
            return {"tipo": "directa", "respuesta": respuesta['output']}
            
        # --- RUTA 2: Análisis Complejo (Mandar a Streamlit) ---
        # --- RUTA 2: Análisis Complejo (Mandar a Streamlit) ---
        elif "TIPO_COMPLEJO" in clasificacion:
            # Enrutamos al Streamlit local
            link_tars = f"http://localhost:8501/?token={consulta.token_sesion}"
            msg = f"Esa consulta requiere visualizaciones avanzadas o procesamiento masivo. 🚀"
            return {"tipo": "redireccion", "respuesta": msg, "link": link_tars}
            
        # --- RUTA 3: KPI Rápido (Agente SQL) ---
        else:
            # Aquí le decimos al Agente SQL que sea súper breve y no genere código UI
            contexto = f"[REGLA]: País: '{consulta.pais}', Marca: '{consulta.marca}'. DIRECTIVA EXTRA: Responde en máximo 2 líneas, sin código Python, solo el dato exacto. Pregunta: {consulta.pregunta}"
            respuesta = agente_tars.invoke({"input": contexto})
            return {"tipo": "directa", "respuesta": respuesta['output']}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    # Corre el servidor en el puerto 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)