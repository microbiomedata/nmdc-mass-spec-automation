"""
This file is for LLM components and related functionalities.
"""

# OpenAI imports
from agents import Agent, Runner, set_tracing_disabled, OpenAIResponsesModel
from agents.mcp import MCPServerStdio, MCPServerStdioParams
from openai import AsyncOpenAI


# Standard library imports
import os
import asyncio


class LLMClient():
    """
    Client for interacting with the LLM API.
    Attributes:

        client (AsyncOpenAI): The OpenAI client instance.
    """
    def __init__(self, mcp_servers=None):
        API_KEY = os.getenv("AI_INCUBATOR_API_KEY")
        self.model_name = "gemini-2.5-flash-project"
        self.base_url = "https://ai-incubator-api.pnnl.gov"
        client = AsyncOpenAI(base_url=self.base_url, api_key=API_KEY)
        self.client = client
        self.model_object = OpenAIResponsesModel(model=self.model_name, openai_client=self.client)
        # Allow custom MCP servers, default to protocol_conversion
        if mcp_servers is None:
            self.mcp_servers = [os.path.join(os.path.dirname(__file__), "protocol_conversion/mcp_server.py")]
            print(f"  [LLM] Using default MCP server: {self.mcp_servers[0]}")
        else:
            self.mcp_servers = mcp_servers
            if self.mcp_servers:
                print(f"  [LLM] Using custom MCP servers: {self.mcp_servers}")
            else:
                print("  [LLM] No MCP servers configured")

    async def get_response(self, messages: list, timeout_seconds: int = 300):
        """
        Get a response from the LLM client.

        Parameters
        ----------
            messages (list): A list of messages to send to the model.
            timeout_seconds (int): Maximum time to wait for response (default: 300s/5min)
        Returns
        -------
            The model's response.
        """
        # tracing is not supported in our AI Incubator instance. Must be disabled.
        set_tracing_disabled(disabled=True)
        
        # If no MCP servers, run without them
        if not self.mcp_servers:
            print("    [LLM] No MCP servers, running agent directly...")
            try:
                print(f"    [LLM] Creating agent with model {self.model_name}...")
                agent = Agent(name="Assistant", model=self.model_object)
                print(f"    [LLM] Sending {len(messages)} messages to Runner...")
                import time
                start = time.time()
                result = await asyncio.wait_for(
                    Runner.run(agent, input=messages),
                    timeout=timeout_seconds
                )
                elapsed = time.time() - start
                print(f"    [LLM] Runner completed in {elapsed:.1f}s")
                return result.final_output
            except asyncio.TimeoutError:
                raise TimeoutError(f"LLM response timed out after {timeout_seconds} seconds. Try reducing input size or increasing timeout.")
        
        # With MCP servers
        params = MCPServerStdioParams(command="python", args=self.mcp_servers)

        async with MCPServerStdio(
            params=params,
            client_session_timeout_seconds=120  # Increased from 60
        ) as mcp_server_instance:
            # use the runner to run the agent with our mcp server and custom model client
            try:
                result = await asyncio.wait_for(
                    Runner.run(
                        Agent(
                            name="Assistant",
                            mcp_servers=[mcp_server_instance],
                            model=self.model_object
                        ),
                        input=messages,
                    ),
                    timeout=timeout_seconds
                )
                return result.final_output
            except asyncio.TimeoutError:
                raise TimeoutError(f"LLM response timed out after {timeout_seconds} seconds. Try reducing input size or increasing timeout.")






