import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.document_loaders import PyPDFDirectoryLoader
from langchain_community.vectorstores import FAISS
from langchain_text_splitters import RecursiveCharacterTextSplitter

# Importaciones de la nueva arquitectura moderna (LCEL)
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser

# Cargar las credenciales de OpenAI desde el .env
load_dotenv()

# La ruta donde guardas tus manuales corporativos estáticos
CARPETA_DOCUMENTOS = "Documentos_Negocio" 

class AgenteConocimiento:
    def __init__(self):
        # Usamos GPT-4o con temperatura 0 para respuestas exactas y analíticas
        self.llm = ChatOpenAI(model="gpt-4o", temperature=0)
        self.chain = self._crear_memoria_corporativa()

    def _crear_memoria_corporativa(self):
        # 1. RASTREADOR INDESTRUCTIBLE DE CARPETAS
        ruta_1 = os.path.join(os.getcwd(), "Documentos_Negocio") 
        ruta_2 = os.path.join(os.path.dirname(__file__), "Documentos_Negocio") 
        
        ruta_absoluta = None
        for ruta_prueba in [ruta_1, ruta_2, CARPETA_DOCUMENTOS, "Documentos_Negocio"]:
            if os.path.exists(ruta_prueba) and os.path.isdir(ruta_prueba):
                # Verificar si adentro hay al menos un PDF
                if any(archivo.endswith('.pdf') for archivo in os.listdir(ruta_prueba)):
                    ruta_absoluta = ruta_prueba
                    break
        
        if not ruta_absoluta:
            print("⚠️ Advertencia: No se encontraron PDFs en ninguna ruta de la nube.")
            return None

        # 2. Leer todos los PDFs de la carpeta encontrada
        loader = PyPDFDirectoryLoader(ruta_absoluta)
        documentos = loader.load()

        # 3. Cortar el texto en fragmentos GIGANTES para no perder contexto
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=3000, chunk_overlap=600)
        fragmentos = text_splitter.split_documents(documentos)

        # 4. Convertir a vectores y crear el buscador (Retriever) con Inteligencia MMR
        vectorstore = FAISS.from_documents(documents=fragmentos, embedding=OpenAIEmbeddings())
        retriever = vectorstore.as_retriever(
            search_type="mmr",
            search_kwargs={"k": 30, "fetch_k": 100, "lambda_mult": 0.5}
        )

        # 5. Crear el formato del prompt
        template = """
        Eres TARS, un Auditor Corporativo e Inteligencia Artificial experta en las políticas, 
        manuales y guías de producto de la empresa.
        
        El sistema te enviará una entrada que incluye la [REGLA] de seguridad del usuario 
        (su País y Marca), y luego su pregunta. 
        
        Tu tarea es responder a la pregunta basándote ÚNICAMENTE en el 
        siguiente contexto recuperado de los documentos oficiales.
        
        REGLA DE ORO 1: TIENES ESTRICTAMENTE PROHIBIDO HABLAR EN OTRO IDIOMA QUE NO SEA ESPAÑOL. NO IMPORTA EN QUÉ IDIOMA TE PREGUNTEN O EN QUÉ IDIOMA ESTÉ EL TEXTO, RESPONDE SIEMPRE EN ESPAÑOL CLARO Y EJECUTIVO.
        
        REGLAS DE AUDITORÍA Y BÚSQUEDA PROFUNDA:
        2. DEDUCCIÓN DE SINÓNIMOS (CRÍTICO): Los usuarios usarán términos coloquiales. 
           - Si preguntan por "cancelar un vale", busca en tu contexto palabras como "anular", "rechazar", "invalidar", "no pago", "fraude", "devolución".
           - Si preguntan por "misión y visión", busca también "objetivo de la empresa", "nuestro propósito", "filosofía".
        3. RESPUESTAS DIRECTAS Y NATURALES (¡CRÍTICO!): Si encuentras información relacionada mediante sinónimos (ej. preguntan por anulación y encuentras reglas de "prestanombres"), responde DIRECTAMENTE con la información. TIENES ESTRICTAMENTE PROHIBIDO decir "No encontré el término exacto, pero encontré...". Habla con seguridad, autoridad y ve directo al punto.
        4. NUNCA inventes políticas que no estén en el texto proporcionado. Si de verdad no hay NADA relacionado en el contexto, simplemente indica que los manuales proporcionados no cubren esa información específica.
        5. Mantén un tono ejecutivo, analítico y claro. Si encuentras listas de requisitos, preséntalas con viñetas.

        Contexto oficial extraído de los manuales:
        {context}

        Entrada del usuario: {question}
        """
        prompt = ChatPromptTemplate.from_template(template)

        # Función auxiliar para unir los fragmentos de texto encontrados
        def format_docs(docs):
            return "\n\n---\n\n".join(doc.page_content for doc in docs)

        # 6. LA CADENA MODERNA (LCEL)
        rag_chain = (
            {"context": retriever | format_docs, "question": RunnablePassthrough()}
            | prompt
            | self.llm
            | StrOutputParser()
        )
        
        return rag_chain

    def invoke(self, inputs):
        """
        Esta función adapta la entrada y salida para que sea 100% compatible 
        con Pagina_Web.py.
        """
        if not self.chain:
            return {"output": "⚠️ TARS no encontró documentos en la carpeta Documentos_Negocio para analizar. Por favor, asegúrate de colocar tus PDFs ahí."}

        pregunta_usuario = inputs.get("input", "")
        respuesta_texto = self.chain.invoke(pregunta_usuario)
        
        return {"output": respuesta_texto}

# Instanciamos el agente para que Pagina_Web.py pueda importarlo y usarlo directamente
agente_pdf = AgenteConocimiento()