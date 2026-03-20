# Data Preprocessing System - Docker部署指南

## 前置要求

- Docker >= 20.10
- Docker Compose >= 2.0

## 快速启动

```bash
# 1. 克隆项目后，进入项目目录
cd data_pre_process

# 2. 启动所有服务
docker-compose up -d

# 3. 查看服务状态
docker-compose ps

# 4. 查看日志
docker-compose logs -f
```

## 服务访问

| 服务 | 地址 | 说明 |
|------|------|------|
| 前端 | http://localhost:13000 | 数据预处理系统Web界面 |
| 后端API | http://localhost:8000 | FastAPI后端服务 |
| 后端API Docs | http://localhost:8000/docs | API文档 |
| MinIO Console | http://localhost:9001 | 对象存储管理界面 |
| ClickHouse | http://localhost:8123 | ClickHouse HTTP接口 |

## 常用命令

```bash
# 启动所有服务
docker-compose up -d

# 停止所有服务
docker-compose down

# 重新构建镜像
docker-compose build --no-cache

# 查看服务日志
docker-compose logs -f backend

# 进入后端容器
docker-compose exec backend bash

# 进入MySQL容器
docker-compose exec mysql mysql -uroot -p12345678

# 查看容器资源使用
docker stats

# 删除所有数据卷（重置数据库）
docker-compose down -v
```

## 环境配置

默认配置定义在 `docker-compose.yml` 中。如需自定义，可创建 `.env` 文件：

```bash
# MySQL配置
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_ROOT_PASSWORD=12345678
SYSTEM_DB_NAME=data_preprocess

# MinIO配置
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000

# ClickHouse配置
CK_HOST=clickhouse
CK_PORT=9000
CK_USER=default
CK_PASSWORD=admin
```

## 服务依赖关系

```
frontend (nginx)
    ↓
backend (FastAPI)
    ↓
┌─────┴─────┐
↓     ↓     ↓
mysql minio clickhouse
```

## 数据持久化

- `mysql_data` - MySQL数据库文件
- `minio_data` - MinIO对象存储数据
- `clickhouse_data` - ClickHouse数据

## 健康检查

所有服务都配置了健康检查：

```bash
# 检查后端健康状态
curl http://localhost:8000/health

# 检查MySQL
docker-compose exec mysql mysqladmin ping -h localhost -u root -p12345678

# 检查MinIO
curl http://localhost:9000/minio/health/live

# 检查ClickHouse
curl http://localhost:8123/ping
```

## 故障排查

### 后端无法连接数据库

```bash
# 1. 检查MySQL是否就绪
docker-compose logs mysql

# 2. 等待MySQL完全启动后重试
docker-compose restart backend
```

### 前端无法访问后端API

```bash
# 1. 检查后端是否运行
docker-compose ps

# 2. 检查后端日志
docker-compose logs backend

# 3. 检查nginx配置
docker-compose exec frontend cat /etc/nginx/conf.d/default.conf
```

### 端口冲突

如果本地端口被占用，可修改 `docker-compose.yml` 中的端口映射：

```yaml
services:
  backend:
    ports:
      - "8001:8000"  # 改为8001
  frontend:
    ports:
      - "13001:80"    # 改为13001
```

## 生产环境部署建议

1. 使用Docker Swarm或Kubernetes进行编排
2. 配置外部数据库（不使用容器内MySQL）
3. 使用HTTPS/TLS加密通信
4. 配置日志收集系统（如ELK）
5. 配置监控告警系统（如Prometheus + Grafana）
