from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator


class Client(BaseModel):
    """Модель клиента"""

    name: str = Field(description="Имя клиента")
    number: str = Field(description="Номер клиента")
    address: Optional[str] = Field(default=None, description="Адрес клиента")


class PharmacySchema(BaseModel):
    """Базовая модель аптеки"""

    address: str = Field(description="Адрес аптеки")


class ProductSchema(BaseModel):
    """Базовая модель товара"""

    name: str = Field(description="Наименование товара")


class PharmacyProductSchema(BaseModel):
    """Модель товара в конкретной аптеке"""

    product: ProductSchema
    pharmacy: PharmacySchema
    price: str = Field(description="Цена товара в конкретной аптеке")
    quantity: int = Field(default=1, description="Количество товара")

    @model_validator(mode="before")
    @classmethod
    def flatten_to_nested(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        # Если уже вложено — не трогаем
        if "product" in data and "pharmacy" in data:
            return data
        return {
            "product": {"name": data["name"]},
            "pharmacy": {"address": data["address"]},
            "price": data["price"],
            "quantity": data.get("quantity", 1),
        }


class ItemOrder(BaseModel):
    """Модель товара в заказе"""

    item_name: str = Field(description="Наименование товара")
    price: int = Field(description="Цена товара в конкретной аптеке")
    quantity: int = Field(default=1, description="Количество товара по умолчанию 1")

    def __repr__(self) -> str:
        return f"<ItemOrder({self.item_name=}, {self.price=}, {self.quantity=})>"  # noqa: E225


class Order(BaseModel):
    """Модель заказа."""

    pharmacy_address: str = Field(description="Адрес аптеки")
    delivery_address: str = Field(description="Адрес доставки или самовывоз")
    pharmacy_phone: str = Field(description="Телефон аптеки")
    client_name: str = Field(description="Имя клиента")
    client_number: str = Field(description="Номер клиента")
    payment: str = Field(description="Метод оплаты, например наличные или kaspi")
    items: List[ItemOrder] = Field(description="Перечень всех товаров в заказе")

    def __repr__(self) -> str:
        return (
            f"<Order({self.pharmacy_address=}, "
            f"{self.pharmacy_phone=}, "
            f"{self.delivery_address=}, "
            f"{self.client_name=}), "
            f"{self.client_number=}, "
            f"{self.payment=}, "
            f"{self.items=}>"
        )
