import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool
from app.main import app
from app.core.db import get_session
from app.models.datasource import DataSource
from app.models.task import DataTask
from app.models.audit import AuditLog
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

class TestDataSourceDeletion:
    
    def test_delete_datasource_without_related_tasks(self, client: TestClient, session: Session):
        datasource = DataSource(
            name="test_ds_no_tasks",
            type="mysql",
            connection_info='{"host":"localhost"}'
        )
        session.add(datasource)
        session.commit()
        session.refresh(datasource)
        
        response = client.delete(f"/datasources/{datasource.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "deleted successfully" in data["message"]
        
        verify_response = client.get(f"/datasources/{datasource.id}")
        assert verify_response.status_code == 404
    
    def test_delete_datasource_with_related_tasks_blocked(self, client: TestClient, session: Session):
        datasource = DataSource(
            name="test_ds_with_tasks",
            type="mysql",
            connection_info='{"host":"localhost"}'
        )
        session.add(datasource)
        session.commit()
        session.refresh(datasource)
        
        task = DataTask(
            name="test_task",
            task_type="sync",
            config=json.dumps({"source_id": datasource.id})
        )
        session.add(task)
        session.commit()
        
        response = client.delete(f"/datasources/{datasource.id}")
        assert response.status_code == 409
        data = response.json()
        assert "detail" in data
        detail = data["detail"]
        assert detail["error_code"] == "DATASOURCE_HAS_RELATED_TASKS"
        assert detail["related_tasks_count"] == 1
        assert len(detail["related_tasks"]) == 1
        
        verify_response = client.get(f"/datasources/{datasource.id}")
        assert verify_response.status_code == 200
    
    def test_delete_nonexistent_datasource(self, client: TestClient):
        response = client.delete("/datasources/99999")
        assert response.status_code == 404
    
    def test_get_related_tasks_endpoint(self, client: TestClient, session: Session):
        datasource = DataSource(
            name="test_ds_related",
            type="mysql",
            connection_info='{"host":"localhost"}'
        )
        session.add(datasource)
        session.commit()
        session.refresh(datasource)
        
        for i in range(3):
            task = DataTask(
                name=f"test_task_{i}",
                task_type="sync",
                config=json.dumps({"source_id": datasource.id})
            )
            session.add(task)
        session.commit()
        
        response = client.get(f"/datasources/{datasource.id}/related-tasks")
        assert response.status_code == 200
        data = response.json()
        assert data["datasource_id"] == datasource.id
        assert data["related_tasks_count"] == 3
        assert len(data["related_tasks"]) == 3
    
    def test_multiple_related_tasks_error_message(self, client: TestClient, session: Session):
        datasource = DataSource(
            name="test_ds_multi_tasks",
            type="mysql",
            connection_info='{"host":"localhost"}'
        )
        session.add(datasource)
        session.commit()
        session.refresh(datasource)
        
        for i in range(7):
            task = DataTask(
                name=f"task_{i}",
                task_type="sync",
                config=json.dumps({"source_id": datasource.id})
            )
            session.add(task)
        session.commit()
        
        response = client.delete(f"/datasources/{datasource.id}")
        assert response.status_code == 409
        data = response.json()
        detail = data["detail"]
        assert detail["related_tasks_count"] == 7
        assert "and 2 more" in detail["message"] or len(detail["related_tasks"]) == 7
    
    def test_task_without_source_id_not_counted(self, client: TestClient, session: Session):
        datasource = DataSource(
            name="test_ds_unrelated",
            type="mysql",
            connection_info='{"host":"localhost"}'
        )
        session.add(datasource)
        session.commit()
        session.refresh(datasource)
        
        task = DataTask(
            name="unrelated_task",
            task_type="sync",
            config=json.dumps({"other_field": "value"})
        )
        session.add(task)
        session.commit()
        
        response = client.delete(f"/datasources/{datasource.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

class TestDataSourceCRUD:
    
    def test_create_datasource(self, client: TestClient):
        response = client.post(
            "/datasources/",
            json={
                "name": "new_datasource",
                "type": "mysql",
                "connection_info": '{"host":"localhost","port":3306}'
            }
        )
        assert response.status_code == 200
        
        list_response = client.get("/datasources/")
        assert list_response.status_code == 200
        data = list_response.json()
        assert any(ds["name"] == "new_datasource" for ds in data["data"])
    
    def test_create_duplicate_datasource(self, client: TestClient, session: Session):
        datasource = DataSource(
            name="duplicate_test",
            type="mysql",
            connection_info='{"host":"localhost"}'
        )
        session.add(datasource)
        session.commit()
        
        response = client.post(
            "/datasources/",
            json={
                "name": "duplicate_test",
                "type": "mysql",
                "connection_info": '{"host":"localhost"}'
            }
        )
        assert response.status_code == 400
    
    def test_list_datasources(self, client: TestClient, session: Session):
        for i in range(3):
            ds = DataSource(
                name=f"list_test_{i}",
                type="mysql",
                connection_info='{}'
            )
            session.add(ds)
        session.commit()
        
        response = client.get("/datasources/")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] >= 3
        assert len(data["data"]) >= 3
    
    def test_get_datasource_by_id(self, client: TestClient, session: Session):
        datasource = DataSource(
            name="get_test",
            type="mysql",
            connection_info='{"host":"localhost"}'
        )
        session.add(datasource)
        session.commit()
        session.refresh(datasource)
        
        response = client.get(f"/datasources/{datasource.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "get_test"
