"""
This file is for LLM components and related functionalities.
"""

# OpenAI imports
from agents import Agent, Runner, set_tracing_disabled, OpenAIResponsesModel
from agents.mcp import MCPServerStdio, MCPServerStdioParams
from openai import AsyncOpenAI


# Standard library imports
import os


class LLMClient():
    """
    Client for interacting with the LLM API.
    Attributes:

        client (AsyncOpenAI): The OpenAI client instance.
    """
    def __init__(self):
        API_KEY = os.getenv("AI_INCUBATOR_API_KEY")
        self.model_name = "gemini-2.5-flash-project"
        self.base_url = "https://ai-incubator-api.pnnl.gov"
        client = AsyncOpenAI(base_url=self.base_url, api_key=API_KEY)
        self.client = client
        self.model_object = OpenAIResponsesModel(model=self.model_name, openai_client=self.client)
        self.mcp_servers=[os.path.join(os.path.dirname(__file__), "protocol_conversion/mcp_server.py")]

    async def get_response(self, messages: list):
        """
        Get a response from the LLM client.

        Parameters
        ----------
            messages (list): A list of messages to send to the model.
        Returns
        -------
            The model's response.
        """
        # params to run the mcp server
        params = MCPServerStdioParams(command="python", args=self.mcp_servers)
        # tracing is not supported in our AI Incubator instance. Must be disabled.
        set_tracing_disabled(disabled=True)

        async with MCPServerStdio(
            params=params,
            client_session_timeout_seconds=60
        ) as mcp_server_instance:
            # use the runner to run the agent with our mcp server and custom model client
            result = await Runner.run(
                Agent(
                    name="Assistant",
                    mcp_servers=[mcp_server_instance],
                    model=self.model_object
                ),
                input=messages,
            )
            return result.final_output






