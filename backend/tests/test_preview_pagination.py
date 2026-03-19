import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool
from app.main import app
from app.core.db import get_session
from app.models.synced_table import SyncedTable
import os
import tempfile
import pandas as pd

@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

@pytest.fixture(name="client")
def client_fixture(session: Session):
    def get_session_override():
        return session
    
    app.dependency_overrides[get_session] = get_session_override
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()

@pytest.fixture(name="temp_csv")
def temp_csv_fixture():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'name_{i}' for i in range(1, 101)],
            'value': [i * 10 for i in range(1, 101)]
        })
        df.to_csv(f, index=False)
        temp_path = f.name
    yield temp_path
    os.unlink(temp_path)

class TestPreviewPagination:
    
    def test_preview_default_pagination(self, client: TestClient, temp_csv):
        response = client.get(f"/data-mgmt/preview?path={temp_csv}")
        assert response.status_code == 200
        data = response.json()
        
        assert "pagination" in data
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["pageSize"] == 20
        assert data["pagination"]["total"] == 100
        assert data["pagination"]["totalPages"] == 5
        assert len(data["data"]) == 20
    
    def test_preview_custom_page_size(self, client: TestClient, temp_csv):
        response = client.get(f"/data-mgmt/preview?path={temp_csv}&page=1&pageSize=50")
        assert response.status_code == 200
        data = response.json()
        
        assert data["pagination"]["pageSize"] == 50
        assert data["pagination"]["totalPages"] == 2
        assert len(data["data"]) == 50
    
    def test_preview_second_page(self, client: TestClient, temp_csv):
        response = client.get(f"/data-mgmt/preview?path={temp_csv}&page=2&pageSize=20")
        assert response.status_code == 200
        data = response.json()
        
        assert data["pagination"]["page"] == 2
        assert data["data"][0]["id"] == 21
        assert data["data"][0]["name"] == "name_21"
    
    def test_preview_last_page(self, client: TestClient, temp_csv):
        response = client.get(f"/data-mgmt/preview?path={temp_csv}&page=5&pageSize=20")
        assert response.status_code == 200
        data = response.json()
        
        assert data["pagination"]["page"] == 5
        assert len(data["data"]) == 20
        assert data["data"][-1]["id"] == 100
    
    def test_preview_page_beyond_total(self, client: TestClient, temp_csv):
        response = client.get(f"/data-mgmt/preview?path={temp_csv}&page=10&pageSize=20")
        assert response.status_code == 200
        data = response.json()
        
        assert len(data["data"]) == 0
    
    def test_preview_sort_ascending(self, client: TestClient, temp_csv):
        response = client.get(f"/data-mgmt/preview?path={temp_csv}&page=1&pageSize=10&sortField=value&sortOrder=asc")
        assert response.status_code == 200
        data = response.json()
        
        assert data["sort"]["field"] == "value"
        assert data["sort"]["order"] == "asc"
        assert data["data"][0]["value"] == 10
    
    def test_preview_sort_descending(self, client: TestClient, temp_csv):
        response = client.get(f"/data-mgmt/preview?path={temp_csv}&page=1&pageSize=10&sortField=value&sortOrder=desc")
        assert response.status_code == 200
        data = response.json()
        
        assert data["sort"]["field"] == "value"
        assert data["sort"]["order"] == "desc"
        assert data["data"][0]["value"] == 1000
    
    def test_preview_invalid_sort_order(self, client: TestClient, temp_csv):
        response = client.get(f"/data-mgmt/preview?path={temp_csv}&sortField=value&sortOrder=invalid")
        assert response.status_code == 400
    
    def test_preview_backward_compatible_limit_offset(self, client: TestClient, temp_csv):
        response = client.get(f"/data-mgmt/preview?path={temp_csv}&limit=10&offset=20")
        assert response.status_code == 200
        data = response.json()
        
        assert len(data["data"]) == 10
        assert data["data"][0]["id"] == 21

class TestPreviewEmptyData:
    
    def test_preview_empty_csv(self, client: TestClient):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,value\n")
            temp_path = f.name
        
        try:
            response = client.get(f"/data-mgmt/preview?path={temp_path}")
            assert response.status_code == 200
            data = response.json()
            assert len(data["data"]) == 0
            assert data["pagination"]["total"] == 0
        finally:
            os.unlink(temp_path)

class TestPreviewNonexistentFile:
    
    def test_preview_nonexistent_path(self, client: TestClient):
        response = client.get("/data-mgmt/preview?path=/nonexistent/path/file.csv")
        assert response.status_code == 404
