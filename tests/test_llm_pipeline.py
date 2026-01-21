from types import SimpleNamespace
from unittest.mock import AsyncMock
import asyncio

from nmdc_dp_utils.llm.llm_pipeline import get_llm_yaml_outline


class DummyConversation:
    def __init__(self):
        self.messages = [{}]

    def add_message(self, role, content):
        self.messages.append({"role": role, "content": content})


def test_get_llm_yaml_outline_calls_llm_twice():
    """Pipeline should request initial outline and schema validation sequentially."""
    conversation = DummyConversation()
    client = SimpleNamespace(
        get_response=AsyncMock(side_effect=["initial-outline", "validated-outline"])
    )

    result = asyncio.run(get_llm_yaml_outline(llm_client=client, conversation_obj=conversation))

    assert result == "validated-outline"
    assert client.get_response.await_count == 2
    assert "Generate the YAML outline" in conversation.messages[1]["content"]
    assert conversation.messages[2]["content"] == "initial-outline"
    assert "validate the generated YAML outline" in conversation.messages[3]["content"]
