from fastapi import FastAPI

from src.api.v1 import endpoints
from src.common.middlewares.middleware_register import register_middlewares


app = FastAPI(
    title="FastAPI Test Salamat",
    version="0.1.0",
)

app.include_router(endpoints.router, prefix="/api/v1")
register_middlewares(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
