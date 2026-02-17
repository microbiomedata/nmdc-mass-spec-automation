"""
LLM-powered tools for NMDC mass spectrometry data processing.

This module provides two main LLM-based workflows:
1. Protocol Conversion: Lab protocol text → YAML outline
2. Biosample Mapping: Raw files + YAML + biosamples → mapping CSV

Both workflows use the same shared infrastructure (LLMClient, ConversationManager)
and curated examples for few-shot learning.
"""

from nmdc_dp_utils.llm.llm_client import LLMClient
from nmdc_dp_utils.llm.llm_conversation_manager import ConversationManager

__all__ = ["LLMClient", "ConversationManager"]
