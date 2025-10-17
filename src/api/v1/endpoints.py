from typing import Annotated, Any, Dict

from fastapi import APIRouter, Depends, HTTPException

# from sqlalchemy import text, func, select
from sqlalchemy.orm import Session
from starlette import status
from starlette.requests import Request

from src.common.tools.ReAct_agent import agent
from src.db.CRUD import (
    # create_db,
    # drop_db,
    update_db,
    # update_vector_store,
)
from src.db.database import get_db

# from src.db.Models import Pharmacy, Product
from src.common.logger import logger

router: APIRouter = APIRouter()
logger.info("Starting app .....")


@router.get("/ask_llm", tags=["Agent"])
async def ask_agent(
    request: Request,
) -> Any:
    try:
        body = await request.json()
        user_input = body.get("user_input", None)
        thread_id = body.get("thread_id", None)
        if user_input and thread_id:
            inputs = {"messages": [("user", user_input)]}
            config = {
                "configurable": {"thread_id": thread_id, "recursionlimit": 100},
            }
        else:
            raise ValueError

    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON body"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {e}",
        )

    try:
        answer = agent.invoke(inputs, config=config)
        ai_answer = answer["messages"][-1].content
    except AttributeError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected response format from agent",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

    return {"answer": ai_answer}


# @router.get("/status_DB", tags=["database"])
# async def get_postgres_db_status(
#     get_db_session: Annotated[Session, Depends(get_db)], request: Request
# ) -> Dict[str, Any]:
#     try:
#         version = get_db_session.scalar(text("SELECT version();"))
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Error connecting to DB {e}",
#         )
#     return {"status": status.HTTP_200_OK, "DB_version": version}


# @router.get("/get_amount_products", tags=["database"])
# async def get_amount_products(
#     db: Annotated[Session, Depends(get_db)],
# ) -> Dict[str, Any]:
#     amount_products = db.scalar(select(func.count()).select_from(Product))
#     return {
#         "status_code": status.HTTP_200_OK,
#         "message": f"Total products: {amount_products}",
#     }


# @router.get("/get_all_pharmacies", tags=["database"])
# async def get_all_pharmacies(db: Annotated[Session, Depends(get_db)]) -> Dict[str, Any]:
#     pharmacies = db.scalars(select(Pharmacy)).all()
#     return {"status_code": 201, "message": f"Total pharmacies: {pharmacies}"}


# @router.post("/create_DB", tags=["database"])
# async def create_tables() -> Dict[str, Any]:
#     try:
#         message = create_db()
#     except Exception:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST, detail="Error creating tables"
#         )
#     else:
#         return {"status_code": status.HTTP_200_OK, "transaction": f"{message}"}
#

@router.post("/update_DB", tags=["database"])
async def update_db_from_1c(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
) -> Dict[str, Any]:
    try:
        json_data = await request.json()
        amount_updated = update_db(db, json_data=json_data if json_data else None)
    except Exception as e:
        if isinstance(e, ValueError):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Invalid JSON body"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error",
            )
    return {
        "status_code": status.HTTP_202_ACCEPTED,
        "message": f"Total updated: {amount_updated}",
    }


# @router.delete("/drop_DB", tags=["delete DB"])
# async def delete_db() -> Dict[str, Any]:
#     message = drop_db()
#     return {"status_code": 201, "message": f"{message}"}


# @router.get("/get_product", tags=["get_product check"])
# async def get_product(
#     request: Request, db: Annotated[Session, Depends(get_db)], product_name: str
# ) -> Dict[str, Any]:
#     products = db.scalars(
#         select(Product).where(Product.name.ilike(f"%{product_name.lower()}%"))
#     )
#
#     if products:
#         return {
#             "product": [product.name for product in products],
#         }
#     else:
#         return {"message": f"Product {product_name} not found!"}
#
#
# @router.get("/update_vector", tags=["update vector"])
# async def update_vector() -> Dict[str, str]:
#     status_message = update_vector_store()
#     return {"status message": status_message}
