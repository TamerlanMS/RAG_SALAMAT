import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()

if not os.getenv("OPENAI_API_KEY"):
    raise ValueError("OPENAI_API_KEY not found in environment variables")

if not os.getenv("PINECONE_API_KEY"):
    raise ValueError("PINECONE_API_KEY not found in environment variables")

if not os.getenv("API_TOKEN"):
    raise ValueError("API_TOKEN not found in environment variables")


def _get_system_prompt() -> str:
    file_path = Path(__file__).resolve().parent / "system_prompt.txt"

    if not file_path.exists():
        raise FileNotFoundError(f"Файл {file_path} не найден.")

    with open(file_path, "r", encoding="utf-8") as f:
        sys_prompt = f.read()

    return sys_prompt


AGENT_PROMPT = _get_system_prompt()
MAX_HISTORY_LENGTH = 15


class OpenAIModel(BaseModel):
    """Базовый класс для языковой модели OpenAI."""

    openai_api_key: Optional[str] = Field(
        default=os.getenv("OPENAI_API_KEY"), description="API-ключ OpenAI"
    )
    timeout: Optional[int] = Field(default=5, description="Таймаут запроса в секундах")


class LLMSettings(OpenAIModel):
    """Конфигурация для языковой модели OpenAI."""

    chat_model: str = Field(
        default="gpt-4o-mini", description="Название модели для чата"
    )
    temperature: float = Field(
        default=0.01, ge=0.0, le=1.0, description="Креативность модели"
    )
    max_tokens: Optional[int] = Field(
        default=5000, description="Максимальное число токенов в ответе"
    )
    system_prompt: Optional[str] = Field(
        default=AGENT_PROMPT, description="Системный промпт для модели"
    )


class PineconeSettings(BaseModel):
    dimension: int = Field(default=1536, description="Размерность эмбеддинга")
    index_name: str = Field(
        default="salamat-names", description="Название индекса в Pinecone"
    )
    index_host: str = Field(
        default="https://salamat-names-rkhbx77.svc.gcp-europe-west4-de1d.pinecone.io",
        description="Хост индекса в Pinecone",
    )
    namespace: str = Field(
        default="price without costs", description="Пространство имен в Pinecone"
    )
    # Embeddings
    embedding_model: str = Field(
        default="text-embedding-3-small", description="Название модели для эмбеддинга"
    )
    openai_api_key: Optional[str] = Field(
        default=os.getenv("OPENAI_API_KEY"), description="API-ключ"
    )

    pinecone_api_key: Optional[str] = Field(
        default=os.getenv("PINECONE_API_KEY"),
        description="API-ключ Pinecone",
    )
    # Text splitting
    chunk_size: int = Field(default=200, description="Размер чанка для разбивки текста")
    chunk_overlap: int = Field(default=50, description="Пересечение чанков")
    # Search config
    search_k: int = Field(default=10, description="Количество результатов поиска")
