from fastapi.testclient import TestClient

from src.main import app

client = TestClient(app)


# def test_ping() -> None:
#     response = client.get("/api/v1/ping")
#     assert response.status_code == 200
#     assert response.json() == {"message": "pong"}
