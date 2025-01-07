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
                FunctionTool.from_defaults(fn=rag_tools.getBooksFromAuthor),
            ]
        else:
            self.tools = tools
        # Crear agente y ejecutor
        self.agent = ReActAgent.from_tools(self.tools, llm=self.llm, verbose=True, max_iterations=30)  # type: ignore

    def send_msg(self, message: str):
        # Generar respuesta del agente
        response = self.agent.chat(message)
        return response
