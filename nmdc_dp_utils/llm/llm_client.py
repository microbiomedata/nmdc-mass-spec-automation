"""
This file is for LLM components and related functionalities.
"""

# OpenAI imports
from openai import OpenAI

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
        self.model = "claude-3-7-sonnet-20250219-v1-project"
        self.base_url = "https://ai-incubator-api.pnnl.gov"
        client = OpenAI(base_url=self.base_url, api_key=API_KEY)
        self.client = client







