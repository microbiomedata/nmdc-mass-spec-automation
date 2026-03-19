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
        use_mcp (bool): Whether MCP tools are enabled for agent execution.
        mcp_tool_filter (callable or None): Tool filter to apply to MCP server.
    """
    def __init__(self, use_mcp: bool = True, mcp_tool_filter=None):
        """
        Initialize LLM client.
        
        Parameters
        ----------
        use_mcp : bool, optional
            Whether to enable MCP server tools. Defaults to True.
        mcp_tool_filter : callable or None, optional
            Tool filter for MCP server. Defaults to None (no filtering).
        """
        API_KEY = os.getenv("AI_INCUBATOR_API_KEY")
        self.model_name = "gemini-2.5-flash-project"
        self.base_url = "https://ai-incubator-api.pnnl.gov"
        client = AsyncOpenAI(base_url=self.base_url, api_key=API_KEY)
        self.client = client
        self.model_object = OpenAIResponsesModel(model=self.model_name, openai_client=self.client)
        self.use_mcp = use_mcp
        self.mcp_tool_filter = mcp_tool_filter

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
        
        # If MCP is disabled, run without MCP tools
        if not self.use_mcp:
            print("    [LLM] MCP disabled, running agent directly...")
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
        
        # With MCP enabled
        from pathlib import Path
        
        # Get workspace root for proper module imports
        workspace_root = Path(__file__).parent.parent.parent
        
        # Configure MCP server to run as a module with workspace as cwd
        params = MCPServerStdioParams(
            command="python",
            args=["-m", "nmdc_dp_utils.llm.mcp_server"],
            cwd=str(workspace_root)
        )

        # Build MCP server kwargs
        mcp_kwargs = {
            "params": params,
            "client_session_timeout_seconds": 120
        }
        
        # Add tool filter if provided
        if self.mcp_tool_filter is not None:
            mcp_kwargs["tool_filter"] = self.mcp_tool_filter

        async with MCPServerStdio(**mcp_kwargs) as mcp_server_instance:
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






