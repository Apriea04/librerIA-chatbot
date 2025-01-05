from agents.tools import rag_tools
from llama_index.core.agent import ReActAgent
from llama_index.llms.ollama import Ollama
from typing import Optional
from llama_index.core.tools import FunctionTool


# ============================================================
# Define the agent
# ============================================================
class RagAgent:
    def __init__(
        self,
        model_name: str,
        tools: Optional[list[FunctionTool]] = None,
    ):
        self.llm = Ollama(model=model_name, temperature=0, request_timeout=7 * 60)
        if not tools:
            self.tools = [
                FunctionTool.from_defaults(fn=rag_tools.recommendSimilarBooks),
                FunctionTool.from_defaults(fn=rag_tools.recommendSameGenreAs),
                FunctionTool.from_defaults(fn=rag_tools.recommendSameAuthorAs),
                FunctionTool.from_defaults(fn=rag_tools.getBookDescription),
                FunctionTool.from_defaults(fn=rag_tools.getBooksInfo),
                FunctionTool.from_defaults(fn=rag_tools.getBookReviews),
                FunctionTool.from_defaults(fn=rag_tools.recommendBooksByReviews),
            ]
        else:
            self.tools = tools
        # Crear agente y ejecutor
        self.agent = ReActAgent.from_tools(self.tools, llm=self.llm, verbose=True, max_iterations=30)  # type: ignore

    def send_msg(self, message: str):
        # Generar respuesta del agente
        response = self.agent.chat(message)
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
