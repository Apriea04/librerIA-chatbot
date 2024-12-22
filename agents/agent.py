from langchain_core.prompts import ChatPromptTemplate
from collections import deque
from langchain_ollama import OllamaLLM
from langchain_core.tools import render_text_description
from langchain.agents import AgentExecutor, create_react_agent
from langchain.memory import ConversationBufferMemory


# ============================================================
# Define the agent
# ============================================================
class Agent:
    def __init__(self, model_name: str, prompt_path: str, tools: list):
        self.llm = OllamaLLM(model=model_name)
        self.memory = ConversationBufferMemory(memory_key="chat_history")
        self.tools = tools
        self.chat_history: deque[str] = deque(maxlen=10)  # Historial de mensajes

        # Cargar contenido del prompt
        with open(prompt_path, "r") as file:
            prompt_content = file.read()

        # Crear plantilla inicial de prompt
        self.prompt = ChatPromptTemplate.from_template(template=prompt_content)
        self.prompt = self.prompt.partial(
            tools=render_text_description(self.tools),
            tool_names=", ".join([t.name for t in self.tools]),
            chat_history="",
        )

        # Crear agente y ejecutor
        self.agent = create_react_agent(self.llm, self.tools, self.prompt)
        self.agent_executor = AgentExecutor(
            agent=self.agent,
            tools=self.tools,
            handle_parsing_errors=True,
            verbose=True,
            memory=self.memory,
        )

    def send_msg(self, message: str):
        # Actualizar el historial en el prompt
        formatted_history = "\n".join(self.chat_history)
        self.prompt = self.prompt.partial(chat_history=formatted_history)

        # Generar respuesta del agente
        response = self.agent_executor.invoke(
            {"input": f"{formatted_history}\nUser: {message}"}
        )

        # Agregar el mensaje al historial
        self.chat_history.append(f"User: {message}")

        # Agregar la respuesta del agente al historial
        self.chat_history.append(f"Agent: {response}")

        return response


# ============================================================
# Test the agent
# note:
# - returning the parsing error will lead to the agent trying again
# - if you set verbose True you can see the agent's internal reflection flow
# ============================================================

"""
tools = [weather_forecast]
agent_instance = Agent(model_name="llama3.3", prompt_path="agents/prompts/REACT_agent.txt", tools=tools)

# Probar el agente
print(agent_instance.send_msg("What is the weather in Paris?"))
print(agent_instance.send_msg("And in New York?"))
print(agent_instance.send_msg("dime que fue lo primero que te he preguntado"))
"""
