import re
from typing import Annotated, Any, List, Optional, Sequence, TypedDict

from dotenv import load_dotenv
from langchain_core.messages import AIMessage, BaseMessage, SystemMessage
from langchain_core.tools import BaseTool, tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph, add_messages
from langgraph.prebuilt import ToolNode

from src.common.llm_model import LLM
from src.common.Schemas.pharmacy_schemas import ItemOrder, Order
from src.common.vector_store import vector_store
from src.db.CRUD import (
    get_all_pharmacies_by_product_name,
    get_product_price,
    get_products_by_name,
)
from src.settings.config import AGENT_PROMPT

load_dotenv()


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]


@tool  # type: ignore
def add(a: int, b: int) -> int:
    """Add two integers together."""
    return a + b


@tool  # type: ignore
def check_phone_number(phone_number: str) -> Optional[str]:
    """
    Проверяет и возвращает нормализованный формат: +7XXXXXXXXXX
    Если номер некорректен — возвращает None.
    """
    # Удаляем все символы кроме цифр и +
    cleaned_number = re.sub(r"[^\d+]", "", phone_number)

    # Преобразуем к формату +7XXXXXXXXXX
    if cleaned_number.startswith("+7"):
        number = cleaned_number[2:]
    elif cleaned_number.startswith("8"):
        number = cleaned_number[1:]
    else:
        return None

    if len(number) == 10 and number.isdigit():
        return f"+7{number}"
    return None


@tool  # type: ignore
def find_product_in_vector_store(product_name: str) -> Any:
    """Find similar products in vector store."""
    db_search_result = get_products_by_name(product_name.lower())
    if not db_search_result:
        return vector_store.search(product_name)
    return db_search_result


@tool  # type: ignore
def find_all_pharmacies_by_product(product_name: str) -> str | List[str]:
    """Find all pharmacies by product name. Return str or List[str]"""
    pharmacies = get_all_pharmacies_by_product_name(product_name)
    return [pharmacy.address for pharmacy in pharmacies]


@tool  # type: ignore
def get_current_price_for_product(product_name: str, address: str) -> Any:
    """Get current price for product by name and pharmacy address."""
    product_price = get_product_price(product_name, address)
    return product_price


@tool(parse_docstring=True, args_schema=Order)  # type: ignore
def create_order(
    pharmacy_address: str,
    pharmacy_phone: str,
    delivery_address: str,
    client_name: str,
    client_number: str,
    payment: str,
    items: List[ItemOrder],
) -> str:
    """
    Создать заказ по шаблону.
    В аргументе должны быть данные о заказе:
     Адрес Аптеки,
     Номер WhatsApp Аптеки,
     Адрес доставки,
     Имя клиента,
     Номер клиента,
     Метод оплаты,
     Перечень товаров,
     Код подтверждения
    """
    total = sum(item.quantity * item.price for item in items)

    template = (
        f"Ваш Заказ: "
        f"Адрес Аптеки: {pharmacy_address}\n"
        f"Номер WhatsApp Аптеки: {pharmacy_phone}\n"
        f"Адрес доставки: {pharmacy_address if total < 15000 else delivery_address}\n"
        f"Имя клиента: {client_name}\n"
        f"Номер клиента: {client_number}\n"
        f"Метод оплаты: {payment}\n\n"
        f"Перечень товаров: {items}\n\n"
        f"Итого: {total}=\n"
        f"Код подтверждения V5R2Sn15n4\n"
    )
    return template


tools: List[BaseTool] = [
    add,
    find_product_in_vector_store,
    find_all_pharmacies_by_product,
    get_current_price_for_product,
    check_phone_number,
    create_order,
]
tool_node = ToolNode(tools)

llm = LLM.bind_tools(tools)


def model_call(state: AgentState) -> AgentState:
    system_prompt = SystemMessage(content=AGENT_PROMPT)
    response = llm.invoke([system_prompt] + list(state["messages"]))
    return {"messages": [response]}


def should_continue(state: AgentState) -> str:
    messages = state["messages"]
    last_message = messages[-1]

    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        return "continue"
    return "end"


graph = StateGraph(AgentState)

graph.add_node("agent", model_call)
graph.add_node("tools", tool_node)
graph.set_entry_point("agent")

graph.add_conditional_edges(
    "agent",
    should_continue,
    {
        "continue": "tools",
        "end": END,
    },
)

graph.add_edge("tools", "agent")

agent = graph.compile(checkpointer=InMemorySaver(), debug=True)
