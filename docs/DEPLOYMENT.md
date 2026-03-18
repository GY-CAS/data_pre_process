# 数据预处理系统 - 部署文档

## 目录

1. [环境准备](#环境准备)
2. [服务器配置推荐](#服务器配置推荐)
3. [网络端口规划](#网络端口规划)
4. [部署步骤](#部署步骤)
5. [环境变量配置](#环境变量配置)
6. [常见问题排查](#常见问题排查)
7. [版本更新流程](#版本更新流程)
8. [安全配置建议](#安全配置建议)

---

## 环境准备

### 软件要求

| 软件 | 最低版本 | 推荐版本 |
|------|---------|---------|
| Docker | 20.10+ | 24.0+ |
| Docker Compose | 2.0+ | 2.20+ |
| Git | 2.0+ | 最新版 |

### 验证环境

```bash
# 检查 Docker 版本
docker --version

# 检查 Docker Compose 版本
docker compose version

# 检查 Docker 服务状态
docker info
```

### 依赖服务

部署前需确保以下服务已启动并可访问：

| 服务 | 默认端口 | 说明 |
|------|---------|------|
| MySQL | 3306 | 数据源之一 |
| ClickHouse | 9006 | 数据源之一 |
| MinIO | 9000 | 对象存储服务 |

---

## 服务器配置推荐

### 开发/测试环境

| 配置项 | 最低要求 |
|--------|---------|
| CPU | 2 核 |
| 内存 | 4 GB |
| 磁盘 | 20 GB |
| 操作系统 | Linux / macOS / Windows (WSL2) |

### 生产环境

| 配置项 | 推荐配置 |
|--------|---------|
| CPU | 4 核+ |
| 内存 | 8 GB+ |
| 磁盘 | 100 GB+ SSD |
| 操作系统 | Linux (Ubuntu 22.04 / CentOS 8+) |

---

## 网络端口规划

### 容器端口映射

| 服务 | 容器端口 | 宿主机端口 | 协议 | 说明 |
|------|---------|-----------|------|------|
| 后端 API | 8000 | 8005 | HTTP | FastAPI 服务 |
| 前端服务 | 13000 | 13005 | HTTP | Vite 预览服务 |

### 外部依赖端口

| 服务 | 端口 | 协议 | 说明 |
|------|------|------|------|
| MySQL | 3306 | TCP | 数据库服务 |
| ClickHouse | 9006 | TCP | 列式数据库 |
| ClickHouse HTTP | 8123 | HTTP | ClickHouse HTTP 接口 |
| MinIO | 9000 | HTTP | 对象存储 API |
| MinIO Console | 9001 | HTTP | MinIO 管理界面 |

### 防火墙配置

```bash
# 开放服务端口 (Linux)
sudo firewall-cmd --permanent --add-port=8005/tcp
sudo firewall-cmd --permanent --add-port=13005/tcp
sudo firewall-cmd --reload

# 或使用 iptables
sudo iptables -A INPUT -p tcp --dport 8005 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 13005 -j ACCEPT
```

---

## 部署步骤

### 1. 获取代码

```bash
git clone <repository_url>
cd data_pre_process
```

### 2. 配置环境变量

```bash
# 复制环境变量模板
cp .env.docker.example .env.docker

# 编辑配置文件
vim .env.docker
```

### 3. 构建镜像

```bash
# 构建所有服务镜像
docker compose build

# 或单独构建
docker compose build backend
docker compose build frontend
```

### 4. 启动服务

```bash
# 启动所有服务
docker compose up -d

# 查看启动日志
docker compose logs -f
```

### 5. 验证部署

```bash
# 检查容器状态
docker compose ps

# 检查后端健康状态
curl http://localhost:8005/health

# 检查数据库连接状态
curl http://localhost:8005/health/db

# 检查前端服务
curl http://localhost:13005

# 检查 API 文档
curl http://localhost:8005/docs
```

### 预期输出

**健康检查成功响应**:
```json
{
  "status": "healthy",
  "service": "data-preprocessing-api"
}
```

**数据库连接检查成功响应**:
```json
{
  "status": "healthy",
  "connections": {
    "mysql": {"status": "reachable", "host": "host.docker.internal", "port": 3306},
    "clickhouse": {"status": "reachable", "host": "host.docker.internal", "port": 9006},
    "minio": {"status": "reachable", "endpoint": "http://host.docker.internal:9000"}
  }
}
```

---

## 环境变量配置

### 环境变量文件模板

创建 `.env.docker` 文件：

```env
# ============================================
# 数据预处理系统 - Docker 环境配置
# ============================================

# MySQL Configuration
MYSQL_HOST=host.docker.internal
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_DB=test_db

# ClickHouse Configuration
CK_HOST=host.docker.internal
CK_PORT=9006
CK_USER=admin
CK_PASSWORD=your_clickhouse_password

# MinIO Configuration
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=your_minio_password
MINIO_ENDPOINT=http://host.docker.internal:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=your_minio_secret

# Backend Service URL (for frontend - browser accessible)
# Use localhost since frontend runs in browser
BACKEND_SERVICE_URL=http://localhost:8005
```

### 环境变量说明

| 变量名 | 说明 | 默认值 | 必填 |
|--------|------|--------|------|
| `MYSQL_HOST` | MySQL 服务器地址 | `host.docker.internal` | ✓ |
| `MYSQL_PORT` | MySQL 端口 | `3306` | ✓ |
| `MYSQL_USER` | MySQL 用户名 | `root` | ✓ |
| `MYSQL_PASSWORD` | MySQL 密码 | - | ✓ |
| `MYSQL_DB` | MySQL 数据库名 | `test_db` | ✓ |
| `CK_HOST` | ClickHouse 服务器地址 | `host.docker.internal` | ✓ |
| `CK_PORT` | ClickHouse 端口 | `9006` | ✓ |
| `CK_USER` | ClickHouse 用户名 | `admin` | ✓ |
| `CK_PASSWORD` | ClickHouse 密码 | - | ✓ |
| `MINIO_ENDPOINT` | MinIO 服务端点 | `http://host.docker.internal:9000` | ✓ |
| `MINIO_ROOT_USER` | MinIO 用户名 | `minioadmin` | ✓ |
| `MINIO_ROOT_PASSWORD` | MinIO 密码 | - | ✓ |
| `MINIO_ACCESS_KEY` | MinIO Access Key | `minioadmin` | ✓ |
| `MINIO_SECRET_KEY` | MinIO Secret Key | - | ✓ |
| `BACKEND_SERVICE_URL` | 后端服务地址（前端使用） | `http://localhost:8005` | ✓ |

### 特殊地址说明

| 地址 | 说明 | 使用场景 |
|------|------|---------|
| `host.docker.internal` | 宿主机在容器内的 DNS 名称 | 容器访问宿主机服务 |
| `localhost` | 本地回环地址 | 浏览器访问本地服务 |

---

## 常见问题排查

### 1. 容器无法启动

**症状**: 容器启动后立即退出

**排查步骤**:
```bash
# 查看容器日志
docker compose logs backend
docker compose logs frontend

# 查看容器状态
docker compose ps -a

# 查看详细错误信息
docker compose up --no-detach
```

**常见原因与解决方案**:

| 原因 | 解决方案 |
|------|---------|
| 端口被占用 | 修改 docker-compose.yml 中的端口映射 |
| 环境变量错误 | 检查 .env.docker 文件配置 |
| 镜像构建失败 | 执行 `docker compose build --no-cache` |
| 依赖服务未启动 | 先启动 MySQL/ClickHouse/MinIO |

### 2. 数据库连接失败

**症状**: 后端日志显示数据库连接错误

**排查步骤**:
```bash
# 进入后端容器
docker exec -it dataprocess_backend bash

# 测试 MySQL 连接
nc -zv host.docker.internal 3306

# 测试 ClickHouse 连接
nc -zv host.docker.internal 9006

# 测试 MinIO 连接
nc -zv host.docker.internal 9000

# 使用 API 检查连接状态
curl http://localhost:8005/health/db
```

**解决方案**:
- 确认数据库服务已启动
- 检查防火墙是否开放端口
- 验证环境变量中的连接信息
- 确认数据库用户权限

### 3. 前端无法访问后端 API

**症状**: 浏览器控制台显示 CORS 错误或连接超时

**排查步骤**:
```bash
# 检查后端服务状态
curl http://localhost:8005/health

# 检查前端配置的后端地址
docker exec dataprocess_frontend cat /app/dist/index.html | grep BACKEND_URL

# 检查浏览器网络请求
# 打开浏览器开发者工具 -> Network 标签
```

**解决方案**:
- 确认 `BACKEND_SERVICE_URL` 配置为 `http://localhost:8005`
- 检查后端 CORS 配置（已配置 `allow_origins=["*"]`）
- 验证端口映射正确

### 4. 健康检查失败

**症状**: 容器状态显示 `unhealthy`

**排查步骤**:
```bash
# 查看健康检查状态
docker inspect --format='{{json .State.Health}}' dataprocess_backend | python -m json.tool

# 手动执行健康检查
docker exec dataprocess_backend curl -f http://localhost:8000/health

# 查看容器日志
docker logs dataprocess_backend --tail 100
```

### 5. Windows 环境特有问题

**症状**: `host.docker.internal` 无法解析

**解决方案**:
```yaml
# docker-compose.yml 中已配置
extra_hosts:
  - "host.docker.internal:host-gateway"
```

---

## 版本更新流程

### 1. 标准更新流程

```bash
# 1. 备份当前配置
cp .env.docker .env.docker.backup

# 2. 拉取最新代码
git pull origin main

# 3. 停止服务
docker compose down

# 4. 重新构建镜像
docker compose build --no-cache

# 5. 启动服务
docker compose up -d

# 6. 验证服务
curl http://localhost:8005/health
```

### 2. 滚动更新（零停机）

```bash
# 更新后端服务
docker compose build backend
docker compose up -d --no-deps backend

# 等待后端健康检查通过
sleep 30

# 更新前端服务
docker compose build frontend
docker compose up -d --no-deps frontend
```

### 3. 回滚操作

```bash
# 查看镜像历史
docker images | grep data_pre_process

# 标记当前版本
docker tag data_pre_process-backend:latest data_pre_process-backend:backup

# 回滚到指定版本
docker compose down
docker tag data_pre_process-backend:previous_version data_pre_process-backend:latest
docker compose up -d
```

---

## 安全配置建议

### 1. 网络隔离

```yaml
# docker-compose.yml
networks:
  frontend_network:
    driver: bridge
  backend_network:
    driver: bridge
    internal: true  # 禁止外部访问
```

### 2. 敏感信息管理

```bash
# 设置文件权限
chmod 600 .env.docker

# 使用 Docker Secrets (Swarm 模式)
echo "your_password" | docker secret create mysql_password -
```

### 3. 容器安全配置

```yaml
# docker-compose.yml
services:
  backend:
    security_opt:
      - no-new-privileges:true
    read_only: true
    tmpfs:
      - /tmp
```

### 4. HTTPS 配置（生产环境推荐）

使用 Nginx 反向代理：

```nginx
server {
    listen 443 ssl;
    server_name your-domain.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://localhost:13005;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    location /api/ {
        proxy_pass http://localhost:8005/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

---

## 附录

### A. 目录结构

```
data_pre_process/
├── backend/
│   ├── app/
│   │   ├── api/
│   │   │   ├── datasource.py
│   │   │   ├── task.py
│   │   │   ├── audit.py
│   │   │   └── data_management.py
│   │   ├── core/
│   │   │   ├── config.py
│   │   │   └── database.py
│   │   ├── models/
│   │   └── main.py
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── api.js
│   │   ├── App.jsx
│   │   └── main.jsx
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
├── docker/
│   ├── Dockerfile.backend
│   ├── Dockerfile.frontend
│   └── frontend_entrypoint.sh
├── docker-compose.yml
├── .env.docker
├── DEPLOYMENT.md
└── USER_MANUAL.md
```

### B. 常用命令速查

| 操作 | 命令 |
|------|------|
| 启动服务 | `docker compose up -d` |
| 停止服务 | `docker compose down` |
| 重启服务 | `docker compose restart` |
| 查看日志 | `docker compose logs -f` |
| 查看状态 | `docker compose ps` |
| 进入容器 | `docker exec -it dataprocess_backend bash` |
| 重新构建 | `docker compose build --no-cache` |
| 资源监控 | `docker stats` |

### C. 联系支持

如遇到无法解决的问题，请提供以下信息：

1. 容器状态：`docker compose ps`
2. 容器日志：`docker compose logs --tail 200`
3. 环境信息：`docker info`
4. 错误截图或描述
