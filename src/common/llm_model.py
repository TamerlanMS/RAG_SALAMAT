from langchain_core.language_models import LanguageModelLike
from langchain_openai import ChatOpenAI

from src.settings.config import LLMSettings


def init_openai_llm() -> LanguageModelLike:
    """
    Initialize the LLM model with specified parameters.

    Returns:
        ChatOpenAI: Initialized chat model instance
    """
    try:
        settings = LLMSettings()
        return ChatOpenAI(
            model_name=settings.chat_model,
            temperature=settings.temperature,
            max_tokens=settings.max_tokens,
            openai_api_key=settings.openai_api_key,
        )
    except Exception as e:
        raise e


# Initialize the default LLM instance and system prompt
LLM = init_openai_llm()
