## 后端启动
docker run -d -p 8005:8000 --name backend-container \
    -e  MYSQL_HOST=localhost \
    -e  MYSQL_PORT=3306 \
    -e  MYSQL_USER=root \
    -e  MYSQL_PASSWORD= infini_rag_flow \
    -e  MYSQL_DB=test_db \
    -e  CK_HOST=localhost \
    -e  CK_PORT=9006 \
    -e  CK_USER=admin \
    -e  CK_PASSWORD=admin \
    -e  MINIO_ENDPOINT=http://localhost:9000 \
    -e  MINIO_ROOT_USER=minioadmin \
    -e  MINIO_ROOT_PASSWORD=minioadmin \
    -e  MINIO_ACCESS_KEY=minioadmin \
    -e  MINIO_SECRET_KEY=minioadmin \
  dataprocess_backend:0315


## 前端启动
docker run -d -p 13005:13000 --name frontend-contain \
-e BACKEND_SERVICE_URL=http://localhost:8005 \
frontend:v1

docker run --rm -p 13005:13000 --name frontend-03 -e BACKEND_SERVICE_URL=http://localhost:8005 dataprocess_frontend:frontend:v1 



 
dataprocess_backend:v0315         13150e512fd6       1.87GB          676MB  
dataprocess_frontend:v0315        7c2a215b1d21 