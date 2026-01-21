import io
import builtins
import pytest
from nmdc_dp_utils.llm.llm_conversation_manager import (
    ConversationManager,
    PROTOCOL_SYSTEM_PROMPT,
)


def test_conversation_manager_rejects_unknown_interaction():
    """Conversation manager should raise if interaction type is unsupported."""
    with pytest.raises(ValueError):
        ConversationManager(interaction_type="unknown")


def test_protocol_conversion_init_adds_system_prompt(monkeypatch):
    """protocol_conversion mode should seed messages and load examples."""
    tracker = {"called": False}

    def fake_loader(self):
        tracker["called"] = True

    monkeypatch.setattr(
        ConversationManager,
        "add_protocol_desc_and_json_examples",
        fake_loader,
    )

    manager = ConversationManager(interaction_type="protocol_conversion")

    assert manager.messages[0] == {}
    assert manager.messages[1] == {"role": "system", "content": PROTOCOL_SYSTEM_PROMPT}
    assert tracker["called"] is True


def test_add_protocol_description_appends_expected_message():
    """Helper should append system message containing the protocol description."""
    manager = ConversationManager.__new__(ConversationManager)
    manager.messages = [{}]
    manager.add_message = ConversationManager.add_message.__get__(manager, ConversationManager)

    description = "Step 1: Prepare buffer"
    manager.add_protocol_description(description=description)

    new_message = manager.messages[-1]
    assert new_message["role"] == "system"
    assert description in new_message["content"]


@pytest.mark.usefixtures("clean_environment")
def test_add_protocol_examples_reads_pairs(monkeypatch):
    """Reading curated examples should append paired system messages per folder."""
    manager = ConversationManager.__new__(ConversationManager)
    manager.messages = [{}]
    manager.add_message = ConversationManager.add_message.__get__(manager, ConversationManager)

    file_map = {}
    for idx in range(1, 8):
        prefix = f"nmdc_dp_utils/llm/llm_protocol_context/example_{idx}"
        file_map[f"{prefix}/extracted_text.txt"] = f"Protocol description {idx}"
        file_map[f"{prefix}/combined_outline.yaml"] = f"outline: example_{idx}"

    real_open = builtins.open

    class _IOWrapper:
        def __init__(self, data):
            self.stream = io.StringIO(data)

        def __enter__(self):
            return self.stream

        def __exit__(self, exc_type, exc, exc_tb):
            self.stream.close()
            return False

    def fake_open(path, *args, **kwargs):
        if path in file_map:
            return _IOWrapper(file_map[path])
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)

    ConversationManager.add_protocol_desc_and_json_examples(manager)

    # 7 folders * 2 messages per folder
    assert len(manager.messages) == 1 + 14
    first_example = manager.messages[1]
    second_example = manager.messages[2]
    assert "Protocol description 1" in first_example["content"]
    assert "outline: example_1" in second_example["content"]
