import json
from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional, Union

import requests  # type: ignore
from fastapi import Depends
from pydantic import TypeAdapter
from sqlalchemy import select, delete
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from src.common.Schemas.pharmacy_schemas import PharmacyProductSchema
from src.common.logger import logger
from src.common.vector_store import vector_store
from src.db.database import engine, get_db
from src.db.Models import Base, Pharmacy, PharmacyProduct, Product


def create_db() -> str:
    """
    Создает базу данных и таблицы, если они не существуют.
    Если база уже существует, ничего не делает.

    :return: Сообщение о результате операции
    """
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as exp:
        if "already exists" in str(exp):
            logger.info("Database already exists")
            return "Database already exists"
        else:
            logger.error("Failed to create database: %s", exp)
            raise
    else:
        return "Database created successfully"


# def drop_db() -> str:
#     """
#     Удаляет все таблицы из базы данных.
#
#     :return: Сообщение о результате операции
#     """
#     try:
#         Base.metadata.drop_all(bind=engine)
#     except Exception as exp:
#         if "does not exist" in str(exp):
#             logger.info("Database does not exist")
#             return "Database does not exist"
#         else:
#             logger.error("Failed to drop database: %s", exp)
#             raise
#     else:
#         return "Database dropped successfully"


def __get_json_from_url(
    address: str,
    params: Optional[Dict[Any, Any]] = None,
    headers: Optional[Dict[Any, Any]] = None,
) -> Any:
    """
    Отправляет GET-запрос по указанному URL и возвращает ответ в формате JSON.

    :param address: URL для запроса
    :param params: (опционально) параметры запроса
    :param headers: (опционально) заголовки запроса
    :return: Ответ в формате dict (JSON)
    :raises: requests.RequestException, ValueError
    """
    response = requests.get(address, params=params, headers=headers)
    response.raise_for_status()  # выбросит исключение, если код ответа не 2xx
    return response.json()


def __get_pharmacy_products_from_json(
    json_data: Optional[Dict[Any, Any]],
) -> List[PharmacyProductSchema]:
    """
    Получает список PharmacyProductSchema из JSON-данных.
    :param json_data: Словарь с ключом "Products", содержащим список продуктов
    :return: Список PharmacyProductSchema
    """
    if not json_data:
        raise ValueError(
            "JSON data is required. Please provide a valid JSON dictionary."
        )
    pharmacy_products = TypeAdapter(List[PharmacyProductSchema]).validate_python(
        json_data["Products"]
    )
    return pharmacy_products


def update_db(
    db: Annotated[Session, Depends(get_db)],
    json_url: str = "https://salamat.cloud1c.pro/FileGPT/SalamatProducts.json",
    json_data: Optional[Dict[Any, Any]] = None,
) -> int:
    """
    Обновляет базу данных с bulk-операциями для ускорения массовой загрузки.

    :param json_url: URL с JSON-данными
    :param json_data: (опционально) JSON-данные
    :param db: SQLAlchemy session
    :return: Количество добавленных записей
    """
    if not json_data:
        json_data = __get_json_from_url(json_url)
    pydantic_list_of_products = __get_pharmacy_products_from_json(json_data)
    counter = 0

    # Загружаем все существующие продукты и аптеки в память через scalars
    products = db.scalars(select(Product)).all()
    pharmacies = db.scalars(select(Pharmacy)).all()
    existing_products = {p.name: p.id for p in products}
    existing_pharmacies = {p.address: p.id for p in pharmacies}

    # Собираем новые продукты и аптеки, исключая дубликаты внутри пачки
    new_product_names = set()
    new_pharmacy_addresses = set()
    new_products = []
    new_pharmacies = []

    for item in pydantic_list_of_products:
        p_name = item.product.name
        ph_addr = item.pharmacy.address
        if p_name not in existing_products and p_name not in new_product_names:
            new_products.append(Product(name=p_name))
            new_product_names.add(p_name)
        if ph_addr not in existing_pharmacies and ph_addr not in new_pharmacy_addresses:
            new_pharmacies.append(Pharmacy(address=ph_addr))
            new_pharmacy_addresses.add(ph_addr)

    # Bulk insert новых продуктов и аптек
    if new_products:
        db.bulk_save_objects(new_products)
    if new_pharmacies:
        db.bulk_save_objects(new_pharmacies)
    db.commit()

    # Обновим словари id через scalars
    products = db.scalars(select(Product)).all()
    pharmacies = db.scalars(select(Pharmacy)).all()
    existing_products = {p.name: p.id for p in products}
    existing_pharmacies = {p.address: p.id for p in pharmacies}

    # Очищаем таблицу PharmacyProduct
    db.execute(delete(PharmacyProduct))
    db.commit()

    # Формируем уникальные связи
    seen_links = set()
    pharm_prod_prices = []

    for item in pydantic_list_of_products:
        try:
            price_product = int(item.price)
        except Exception as exp:
            logger.error("Price error: %s | Product: %s", exp, item)
            continue

        p_name = item.product.name.strip()
        ph_addr = item.pharmacy.address.strip()
        product_id = existing_products.get(p_name)
        pharmacy_id = existing_pharmacies.get(ph_addr)

        if not product_id or not pharmacy_id:
            continue

        key = (product_id, pharmacy_id)
        if key in seen_links:
            continue  # Пропускаем дубликаты
        seen_links.add(key)

        pharm_prod_prices.append(
            PharmacyProduct(
                product_id=product_id, pharmacy_id=pharmacy_id, price=price_product
            )
        )
        counter += 1
    if pharm_prod_prices:
        db.bulk_save_objects(pharm_prod_prices)
    db.commit()

    # Обновление vector store по понедельникам с 4-5 утра
    now = datetime.now()
    if now.weekday() == 0 and (4 <= now.hour <= 5):
        logger.info("Starting to rebuild vector store")
        status_update = update_vector_store()
        logger.info("Vector store rebuilt status: %s", status_update)

    return counter


def get_all_pharmacies_by_product_name(product_name: str) -> Any:
    """
    Поиск аптек по названию продукта
    :param product_name: Название продукта
    :return: Список аптек
    """
    db = next(get_db())
    # Поиск id продукта по имени

    product = db.scalar(select(Product).where(Product.name.ilike(f"%{product_name}%")))
    if not product:
        return []
    # Поиск аптеки, где есть этот продукт
    query = (
        select(Pharmacy)
        .join(PharmacyProduct, Pharmacy.id == PharmacyProduct.pharmacy_id)
        .where(PharmacyProduct.product_id == product.id)
    )
    pharmacies = db.scalars(query).all()
    return pharmacies


def get_product_price(product_name: str, pharmacy_address: str) -> Any:
    """
    Поиск цены продукта в конкретной аптеке
    :param product_name: Название продукта
    :param pharmacy_address: Адрес аптеки
    :return: Цена продукта или None, если не найдено
    """
    db = next(get_db())
    product = db.scalar(select(Product).where(Product.name.ilike(f"%{product_name}%")))
    if not product:
        return None
    pharmacy = db.scalar(select(Pharmacy).where(Pharmacy.address == pharmacy_address))
    if not pharmacy:
        return None
    pharmacy_product = db.scalar(
        select(PharmacyProduct).where(
            PharmacyProduct.product_id == product.id,
            PharmacyProduct.pharmacy_id == pharmacy.id,
        )
    )
    if not pharmacy_product:
        return None
    return pharmacy_product.price


def get_products_by_name(product_name: str) -> Optional[List[str]]:
    db = next(get_db())
    products = db.scalars(
        select(Product).where(Product.name.ilike(f"%{product_name.lower()}%"))
    )
    if products:
        return [product.name for product in products]
    return None


def get_all_products() -> Optional[List[str]]:
    db = next(get_db())
    products = db.scalars(select(Product)).all()
    if products:
        return [product.name for product in products]
    return None


def update_vector_store() -> Any:
    products_names = get_all_products()
    if products_names:
        status_message = vector_store.rebuild_vector_store(
            products_names=products_names
        )
        return status_message
    return "No products found"


def get_pharmacy_phone_by_address(address: str) -> Any:
    db = next(get_db())
    try:
        pharmacy_phone = db.scalar(select(Pharmacy).where(Pharmacy.address == address)).phone
    except NoResultFound:
        return None
    finally:
        db.close()
    return pharmacy_phone


def update_pharmacy_phone_number(pharmacy_data: Union[dict[str, str], str]) -> bool:
    """
    Функция обновляет телефонные номера аптек в базе данных.
    Входящие данные могут быть:
      - словарём: {pharmacy_address: phone_number}
      - или JSON-строкой с таким же содержанием.

    :param pharmacy_data: Словарь или JSON-строка в формате {"адрес": "телефон"}

    :return bool: True, если обновление прошло успешно, иначе False
    """
    if not pharmacy_data:
        return True  # Нечего обновлять

    # Парсим JSON, если входной аргумент - строка
    if isinstance(pharmacy_data, str):
        try:
            data = json.loads(pharmacy_data)
        except json.JSONDecodeError as e:
            logger.error("Некорректный JSON: %s", e)
            return False
    elif isinstance(pharmacy_data, dict):
        data = pharmacy_data
    else:
        logger.error("Ожидался dict или str (JSON), получено: %s", type(pharmacy_data))
        return False

    # Проверяем, что все ключи и значения — строки
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in data.items()):
        logger.error("Все ключи и значения в pharmacy_data должны быть строками")
        return False

    db = next(get_db())
    try:
        # 1-й запрос: Получаем аптеки по адресам из данных
        addresses = list(data.keys())
        pharmacies = db.scalars(select(Pharmacy).where(Pharmacy.address.in_(addresses))).all()

        # Подготавливаем данные для массового обновления
        update_data = []
        for pharmacy in pharmacies:
            new_phone = data.get(pharmacy.address)
            if new_phone is not None:
                update_data.append({
                    "id": pharmacy.id,
                    "phone": new_phone
                })

        # 2-й запрос: Массовое обновление
        if update_data:
            db.bulk_update_mappings(Pharmacy, update_data)
        db.commit()

        return True
    except Exception as e:
        db.rollback()
        logger.error("Ошибка при обновлении телефонов аптек: %s", e)
        return False
    finally:
        db.close()
