from pydantic import BaseModel, Field
from langchain_core.tools import tool
from langchain_core.prompts import ChatPromptTemplate
from collections import deque
import random

# ============================================================
# Define a custom but dummy tool
# ============================================================
@tool
def weather_forecast(location: str):
    """Weather forecast tool."""
    tmp = random.randint(-10, 35)
    print(f"Weather for {location}: {tmp}")
    return f"{tmp}ºC, sunny"

# ============================================================
# Define the agent
# ============================================================
class Agent:
    def __init__(self, model_name: str, prompt_path: str, tools: list):
        from langchain_ollama import OllamaLLM
        from langchain_core.tools import render_text_description
        from langchain.agents import AgentExecutor, create_react_agent

        self.llm = OllamaLLM(model=model_name)
        self.tools = tools
        self.chat_history = deque(maxlen=10)

        with open(prompt_path, "r") as file:
            prompt_content = file.read()

        self.prompt = ChatPromptTemplate.from_template(template=prompt_content)
        self.prompt = self.prompt.partial(
            tools=render_text_description(self.tools),
            tool_names=", ".join([t.name for t in self.tools]),
            chat_history=""
        )

        self.agent = create_react_agent(self.llm, self.tools, self.prompt)
        self.agent_executor = AgentExecutor(agent=self.agent, tools=self.tools, handle_parsing_errors=True, verbose=True)

    def send_msg(self, message: str):
        self.chat_history.append(message)
        self.prompt = self.prompt.partial(chat_history="\n".join(self.chat_history))
        return self.agent_executor.invoke({"input": message})

# ============================================================
# Test the agent
# note: 
# - returning the parsing error will lead to the agent trying again
# - if you set verbose True you can see the agent's internal reflection flow
# ============================================================
tools = [weather_forecast]
agent_instance = Agent(model_name="llama3.3", prompt_path="agents/prompts/REACT_agent.txt", tools=tools)
print(agent_instance.send_msg("What is the weather in Paris?"))
print(agent_instance.send_msg("And in New York?"))