from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import os
import asyncio
import pytest

from nmdc_dp_utils.llm import llm_client as llm_client_module
from nmdc_dp_utils.llm.llm_client import LLMClient


@pytest.fixture(autouse=True)
def enable_api_key(monkeypatch):
    monkeypatch.setenv("AI_INCUBATOR_API_KEY", "test-key")


def test_llm_client_initialization(monkeypatch):
    """Ensure client wires model, base URL, schema path when constructed."""
    async_openai = Mock(name="AsyncOpenAI", return_value="client")
    responses_model = Mock(name="OpenAIResponsesModel", return_value="model")

    monkeypatch.setattr(llm_client_module, "AsyncOpenAI", async_openai)
    monkeypatch.setattr(llm_client_module, "OpenAIResponsesModel", responses_model)

    client = LLMClient()

    async_openai.assert_called_once_with(base_url="https://ai-incubator-api.pnnl.gov", api_key="test-key")
    responses_model.assert_called_once_with(model="gemini-2.5-flash-project", openai_client="client")
    expected_schema_path = os.path.join(
        os.path.dirname(llm_client_module.__file__),
        "llm_protocol_context/schema_server.py",
    )
    assert client.client == "client"
    assert client.model_object == "model"
    assert client.mcp_servers == [expected_schema_path]


def test_llm_client_get_response_invokes_runner(monkeypatch):
    """Verify get_response disables tracing, boots MCP server, and returns output."""
    monkeypatch.setattr(llm_client_module, "AsyncOpenAI", Mock(return_value="client"))
    monkeypatch.setattr(llm_client_module, "OpenAIResponsesModel", Mock(return_value="model"))

    state = {"value": None}

    def fake_set_tracing_disabled(*, disabled):
        state["value"] = disabled

    monkeypatch.setattr(llm_client_module, "set_tracing_disabled", fake_set_tracing_disabled)

    runner_mock = AsyncMock(return_value=SimpleNamespace(final_output="ok"))
    monkeypatch.setattr(llm_client_module.Runner, "run", runner_mock)

    captured_agent = {}

    class DummyAgent:
        def __init__(self, name, mcp_servers, model):
            captured_agent["name"] = name
            captured_agent["mcp_servers"] = mcp_servers
            captured_agent["model"] = model

    monkeypatch.setattr(llm_client_module, "Agent", DummyAgent)

    class DummyParams:
        def __init__(self, command, args):
            self.command = command
            self.args = args

    monkeypatch.setattr(llm_client_module, "MCPServerStdioParams", DummyParams)

    captured_context = {}

    class DummyMCPServer:
        def __init__(self, params, client_session_timeout_seconds):
            captured_context["params"] = params
            captured_context["timeout"] = client_session_timeout_seconds

        async def __aenter__(self):
            return "mcp-instance"

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(llm_client_module, "MCPServerStdio", DummyMCPServer)

    client = LLMClient()

    messages = [{"role": "user", "content": "hi"}]
    result = asyncio.run(client.get_response(messages))

    assert result == "ok"
    assert state["value"] is True
    assert captured_agent == {
        "name": "Assistant",
        "mcp_servers": ["mcp-instance"],
        "model": "model",
    }
    assert captured_context["params"].command == "python"
    assert captured_context["params"].args == client.mcp_servers
    assert captured_context["timeout"] == 60
    assert runner_mock.await_count == 1
    assert runner_mock.await_args.kwargs["input"] == messages
