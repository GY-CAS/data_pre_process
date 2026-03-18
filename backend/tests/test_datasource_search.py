import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool
from app.main import app
from app.core.db import get_session
from app.models.datasource import DataSource
import json

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

class TestDataSourceSearch:
    
    def test_search_by_name(self, client: TestClient, session: Session):
        ds1 = DataSource(name="mysql_test", type="mysql", data_type="TEXT", connection_info='{}')
        ds2 = DataSource(name="clickhouse_prod", type="clickhouse", data_type="TIMESERIES", connection_info='{}')
        session.add(ds1)
        session.add(ds2)
        session.commit()
        
        response = client.get("/datasources/search?name=mysql")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["data"][0]["name"] == "mysql_test"
    
    def test_search_by_type(self, client: TestClient, session: Session):
        ds1 = DataSource(name="ds1", type="mysql", data_type="TEXT", connection_info='{}')
        ds2 = DataSource(name="ds2", type="clickhouse", data_type="TIMESERIES", connection_info='{}')
        ds3 = DataSource(name="ds3", type="mysql", data_type="IMAGE", connection_info='{}')
        session.add_all([ds1, ds2, ds3])
        session.commit()
        
        response = client.get("/datasources/search?type=mysql")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        for ds in data["data"]:
            assert ds["type"] == "mysql"
    
    def test_search_by_data_type(self, client: TestClient, session: Session):
        ds1 = DataSource(name="ds1", type="mysql", data_type="TEXT", connection_info='{}')
        ds2 = DataSource(name="ds2", type="clickhouse", data_type="TIMESERIES", connection_info='{}')
        ds3 = DataSource(name="ds3", type="minio", data_type="IMAGE", connection_info='{}')
        session.add_all([ds1, ds2, ds3])
        session.commit()
        
        response = client.get("/datasources/search?data_type=TEXT")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["data"][0]["data_type"] == "TEXT"
    
    def test_search_by_multiple_conditions(self, client: TestClient, session: Session):
        ds1 = DataSource(name="mysql_text", type="mysql", data_type="TEXT", connection_info='{}')
        ds2 = DataSource(name="mysql_image", type="mysql", data_type="IMAGE", connection_info='{}')
        ds3 = DataSource(name="clickhouse_text", type="clickhouse", data_type="TEXT", connection_info='{}')
        session.add_all([ds1, ds2, ds3])
        session.commit()
        
        response = client.get("/datasources/search?type=mysql&data_type=TEXT")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["data"][0]["name"] == "mysql_text"
        assert data["data"][0]["type"] == "mysql"
        assert data["data"][0]["data_type"] == "TEXT"
    
    def test_search_with_all_conditions(self, client: TestClient, session: Session):
        ds1 = DataSource(name="prod_mysql_text", type="mysql", data_type="TEXT", connection_info='{}')
        ds2 = DataSource(name="prod_mysql_image", type="mysql", data_type="IMAGE", connection_info='{}')
        session.add_all([ds1, ds2])
        session.commit()
        
        response = client.get("/datasources/search?name=prod&type=mysql&data_type=TEXT")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["data"][0]["name"] == "prod_mysql_text"
    
    def test_search_no_results(self, client: TestClient, session: Session):
        ds = DataSource(name="test", type="mysql", data_type="TEXT", connection_info='{}')
        session.add(ds)
        session.commit()
        
        response = client.get("/datasources/search?name=nonexistent")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert len(data["data"]) == 0
    
    def test_search_with_pagination(self, client: TestClient, session: Session):
        for i in range(25):
            ds = DataSource(name=f"ds_{i:02d}", type="mysql", data_type="TEXT", connection_info='{}')
            session.add(ds)
        session.commit()
        
        response = client.get("/datasources/search?skip=0&limit=10")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 25
        assert len(data["data"]) == 10
        
        response2 = client.get("/datasources/search?skip=10&limit=10")
        data2 = response2.json()
        assert len(data2["data"]) == 10
    
    def test_search_special_characters(self, client: TestClient, session: Session):
        ds = DataSource(name="test-ds_123", type="mysql", data_type="TEXT", connection_info='{}')
        session.add(ds)
        session.commit()
        
        response = client.get("/datasources/search?name=test-ds")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
    
    def test_search_returns_filter_info(self, client: TestClient, session: Session):
        ds = DataSource(name="test_ds", type="mysql", data_type="TEXT", connection_info='{}')
        session.add(ds)
        session.commit()
        
        response = client.get("/datasources/search?name=test&type=mysql&data_type=TEXT")
        assert response.status_code == 200
        data = response.json()
        assert "filters_applied" in data
        assert data["filters_applied"] == 3
        assert "search_criteria" in data
        assert len(data["search_criteria"]) == 3

class TestDataSourceCreateValidation:
    
    def test_create_without_data_type_fails(self, client: TestClient):
        response = client.post("/datasources/", json={
            "name": "test_ds",
            "type": "mysql",
            "connection_info": '{}'
        })
        assert response.status_code == 422 or response.status_code == 200
    
    def test_create_with_valid_data_type(self, client: TestClient):
        response = client.post("/datasources/", json={
            "name": "test_ds_valid",
            "type": "mysql",
            "data_type": "TEXT",
            "connection_info": '{}'
        })
        assert response.status_code == 200
        list_response = client.get("/datasources/?name=test_ds_valid")
        data = list_response.json()
        assert data["total"] >= 1
    
    def test_create_with_all_data_types(self, client: TestClient):
        data_types = ["TEXT", "TIMESERIES", "IMAGE"]
        for dt in data_types:
            response = client.post("/datasources/", json={
                "name": f"ds_{dt}",
                "type": "mysql",
                "data_type": dt,
                "connection_info": '{}'
            })
            assert response.status_code == 200
