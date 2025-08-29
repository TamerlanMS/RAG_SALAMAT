from typing import List, Optional

from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pinecone import Pinecone
from src.settings.config import PineconeSettings

settings = PineconeSettings()


class VectorStore:
    def __init__(self) -> None:
        self.config = settings

        self.pc = Pinecone(api_key=self.config.pinecone_api_key)
        self.index = self.pc.Index(
            name=self.config.index_name, host=self.config.index_host
        )

        self.embedding = OpenAIEmbeddings(
            model=self.config.embedding_model,
            dimensions=self.config.dimension,
            openai_api_key=self.config.openai_api_key,
        )

        self.vector_store = PineconeVectorStore(
            index=self.index, embedding=self.embedding, namespace=self.config.namespace
        )

        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.config.chunk_size, chunk_overlap=self.config.chunk_overlap
        )

        self.retriever = self.vector_store.as_retriever(
            search_kwargs={"namespace": self.config.namespace}
        )

    def search(self, query: str) -> str:
        """Поиск по векторной базе через retriever.
        Возвращает текстовые результаты."""
        results = self.retriever.invoke(query)
        # results может быть списком документов или строк
        if isinstance(results, list):
            # Если это список документов, склеим их содержимое
            return "\n".join(str(doc) for doc in results)
        return str(results)

    def __delete(self) -> str:
        try:
            self.index.delete(delete_all=True, namespace=self.config.namespace)
        except Exception as e:
            return f"Index deletion failed with error: {e}"
        return f"Index {self.config.index_name} deleted successfully."

    def rebuild_vector_store(self, products_names: Optional[List[str]]) -> str:
        self.__delete()
        try:
            self.vector_store.add_texts(
                texts=products_names,
                namespace=self.config.namespace,
            )
        except Exception as e:
            if "Index does not exist" in str(e):
                return f"Error: Index {self.config.index_name} does not exist."
            else:
                return f"Error: {e}"
        return "Index rebuilt successfully."


vector_store = VectorStore()
