import logging
import time
from typing import Callable, Awaitable

from fastapi import Request, Response, FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.common.logger import logger


async def debug_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    request_body = await request.body()
    request.state.raw_body = request_body

    # Считаем чистое время ответа
    start_time = time.perf_counter()
    response = await call_next(request)
    response_process_time = round(time.perf_counter() - start_time, 3)

    # Считываем тело ответа и собираем заново
    resp_body = b""
    async for chunk in response.body_iterator:  # type: ignore
        resp_body += chunk

    response_body = resp_body.decode("utf-8", errors="ignore")

    response = Response(
        content=resp_body,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=response.media_type,
    )
    # Логируем запрос и ответ

    body = request_body.decode("utf-8", errors="ignore")
    logger.debug(
        "--> Request from %s %s to %s - request body: %s",
        request.client,
        request.method,
        request.url,
        body,
    )
    logger.debug(
        "<-- Response took %s seconds - response body: %s",
        response_process_time,
        response_body,
    )

    return response


async def log_new_request_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    if logger.isEnabledFor(logging.DEBUG):
        return await debug_middleware(request, call_next)
    logger.info(
        "Request from %s %s to %s",
        request.client,
        request.method,
        request.url,
    )
    return await call_next(request)


def register_middlewares(app: FastAPI) -> None:
    app.middleware("http")(log_new_request_middleware)
    # app.middleware("http")(debug_middleware)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
