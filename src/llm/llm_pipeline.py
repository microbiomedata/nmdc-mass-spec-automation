from llm.llm_client import NMDCLLMClient
from llm.llm_conversation_manager import ConversationManager


def completions(llm_client:NMDCLLMClient, messages:list[dict]):
    response = llm_client.client.chat.completions.create(
        model=llm_client.model,
        messages=messages
    )
    return response


if __name__ == "__main__":
    protocol_description_path = ""
    llm_client = NMDCLLMClient()
    conversation_client = ConversationManager()
    response = completions(llm_client)


