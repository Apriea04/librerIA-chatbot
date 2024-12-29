from agents.rag_agent import RagAgent
from utils.env_loader import EnvLoader
import streamlit as st

def render_ui():
    env = EnvLoader()
    st.set_page_config(page_title="librerIA Chatbot", page_icon="📚")
    st.title("📚 librerIA Chatbot")
    
    # Inicializar el agente RAG
    rag_agent = RagAgent(env.agent_llm_model)
    
    # Estado inicial de sesión
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Mostrar mensajes en la interfaz
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Entrada de usuario
    if user_input := st.chat_input("Escribe tu pregunta o consulta aquí:"):
        # Guardar mensaje del usuario
        st.session_state.messages.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        # Obtener respuesta del agente RAG
        with st.chat_message("assistant"):
            response = rag_agent.send_msg(user_input)
            st.markdown(response)
        
        # Guardar respuesta del asistente
        st.session_state.messages.append({"role": "assistant", "content": response})

# Ejecutar la interfaz
if __name__ == "__main__":
    render_ui()
