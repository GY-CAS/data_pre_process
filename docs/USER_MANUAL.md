# 数据预处理系统 - 使用手册

## 目录

1. [系统概述](#系统概述)
2. [系统架构](#系统架构)
3. [服务访问](#服务访问)
4. [容器管理](#容器管理)
5. [功能模块](#功能模块)
6. [日常维护](#日常维护)
7. [数据源配置指南](#数据源配置指南)
8. [数据备份与恢复](#数据备份与恢复)
9. [故障排除](#故障排除)
10. [附录](#附录)

---

## 系统概述

### 系统简介

数据预处理系统是一个用于数据源管理、数据处理任务执行和审计的企业级应用平台。系统采用前后端分离架构，支持 MySQL、ClickHouse、MinIO 等多种数据源的连接与管理。

### 主要功能

- **数据源管理**: 支持多种数据源的连接配置与测试
- **任务管理**: 创建、执行、监控数据处理任务
- **审计日志**: 记录系统操作日志，支持追溯
- **数据管理**: 数据资产预览、导出、编辑

### 技术栈

| 组件 | 技术 |
|------|------|
| 前端 | React + Vite + Tailwind CSS |
| 后端 | Python + FastAPI + SQLModel |
| 容器化 | Docker + Docker Compose |
| 数据库 | MySQL / ClickHouse |
| 对象存储 | MinIO |

---

## 系统架构

### 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                          用户浏览器                              │
│                    http://localhost:13005                        │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      前端容器 (Frontend)                          │
│                     端口: 13000 → 13005                          │
│                   React + Vite Preview                           │
└─────────────────────────────┬───────────────────────────────────┘
                              │ HTTP API
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      后端容器 (Backend)                           │
│                      端口: 8000 → 8005                           │
│                    FastAPI + Uvicorn                             │
└──────────┬──────────────┬──────────────┬────────────────────────┘
           │              │              │
           ▼              ▼              ▼
    ┌──────────┐   ┌──────────┐   ┌──────────┐
    │  MySQL   │   │ClickHouse│   │  MinIO   │
    │  :3306   │   │  :9006   │   │  :9000   │
    └──────────┘   └──────────┘   └──────────┘
```

### 服务组件

| 组件 | 容器名 | 端口 | 说明 |
|------|--------|------|------|
| 前端服务 | `dataprocess_frontend` | 13005 | Web 用户界面 |
| 后端服务 | `dataprocess_backend` | 8005 | REST API 服务 |

---

## 服务访问

### Web 界面访问

| 服务 | 地址 | 说明 |
|------|------|------|
| 前端界面 | http://localhost:13005 | 用户操作界面 |
| API 文档 | http://localhost:8005/docs | Swagger UI 交互式文档 |
| API 文档 | http://localhost:8005/redoc | ReDoc 文档 |
| 健康检查 | http://localhost:8005/health | 服务健康状态 |
| 数据库状态 | http://localhost:8005/health/db | 数据库连接状态 |

### API 访问示例

```bash
# 健康检查
curl http://localhost:8005/health

# 获取数据源列表
curl http://localhost:8005/datasources/

# 测试数据源连接
curl -X POST http://localhost:8005/datasources/test-connection \
  -H "Content-Type: application/json" \
  -d '{"type":"mysql","host":"localhost","port":3306,"user":"root","password":"password"}'
```

---

## 容器管理

### 启动服务

```bash
# 启动所有服务
docker compose up -d

# 启动指定服务
docker compose up -d backend
docker compose up -d frontend

# 查看启动日志
docker compose logs -f
```

### 停止服务

```bash
# 停止所有服务
docker compose down

# 停止指定服务
docker compose stop backend
docker compose stop frontend

# 停止并删除容器、网络
docker compose down --volumes
```

### 重启服务

```bash
# 重启所有服务
docker compose restart

# 重启指定服务
docker compose restart backend
docker compose restart frontend
```

### 查看服务状态

```bash
# 查看容器状态
docker compose ps

# 查看容器详细信息
docker compose ps -a

# 查看资源使用情况
docker stats dataprocess_backend dataprocess_frontend
```

### 查看日志

```bash
# 查看所有服务日志
docker compose logs

# 实时查看日志
docker compose logs -f

# 查看最近 100 行日志
docker compose logs --tail 100

# 查看指定服务日志
docker compose logs backend
docker compose logs frontend

# 查看带时间戳的日志
docker compose logs -f --timestamps
```

### 进入容器

```bash
# 进入后端容器
docker exec -it dataprocess_backend bash

# 进入前端容器
docker exec -it dataprocess_frontend sh

# 在容器内执行命令
docker exec dataprocess_backend curl http://localhost:8000/health
```

### 容器管理速查表

| 操作 | 命令 |
|------|------|
| 启动 | `docker compose up -d` |
| 停止 | `docker compose down` |
| 重启 | `docker compose restart` |
| 状态 | `docker compose ps` |
| 日志 | `docker compose logs -f` |
| 进入容器 | `docker exec -it <container> bash` |
| 资源监控 | `docker stats` |
| 清理资源 | `docker system prune -a` |

---

## 功能模块

### 1. 数据源管理

**访问路径**: `/datasources`

**功能说明**:
- 创建、编辑、删除数据源配置
- 测试数据源连接
- 支持的数据源类型: MySQL, ClickHouse, MinIO, CSV

**API 端点**:

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/datasources/` | 获取数据源列表 |
| POST | `/datasources/` | 创建数据源 |
| GET | `/datasources/{id}` | 获取数据源详情 |
| PUT | `/datasources/{id}` | 更新数据源 |
| DELETE | `/datasources/{id}` | 删除数据源 |
| POST | `/datasources/test-connection` | 测试连接 |

### 2. 任务管理

**访问路径**: `/tasks`

**功能说明**:
- 创建数据处理任务
- 执行任务并监控进度
- 查看任务执行历史

**API 端点**:

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/tasks/` | 获取任务列表 |
| POST | `/tasks/` | 创建任务 |
| GET | `/tasks/{id}` | 获取任务详情 |
| POST | `/tasks/{id}/execute` | 执行任务 |
| DELETE | `/tasks/{id}` | 删除任务 |

### 3. 审计日志

**访问路径**: `/audit`

**功能说明**:
- 查看系统操作日志
- 按时间、用户、操作类型筛选
- 导出审计报告

**API 端点**:

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/audit/logs` | 获取审计日志 |
| GET | `/audit/export` | 导出审计报告 |

### 4. 数据管理

**访问路径**: `/data-mgmt`

**功能说明**:
- 数据资产预览
- 数据导出
- 数据编辑

**API 端点**:

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/data-mgmt/preview` | 预览数据 |
| POST | `/data-mgmt/export` | 导出数据 |
| PUT | `/data-mgmt/update` | 更新数据 |

---

## 日常维护

### 健康检查

```bash
# 检查后端服务健康状态
curl http://localhost:8005/health

# 检查数据库连接状态
curl http://localhost:8005/health/db

# 检查前端服务
curl http://localhost:13005
```

### 资源监控

```bash
# 查看容器资源使用
docker stats --no-stream

# 查看磁盘使用
docker system df

# 查看容器进程
docker top dataprocess_backend
```

### 日志管理

```bash
# 清理 Docker 日志
truncate -s 0 $(docker inspect --format='{{.LogPath}}' dataprocess_backend)
truncate -s 0 $(docker inspect --format='{{.LogPath}}' dataprocess_frontend)

# 配置日志轮转 (docker-compose.yml)
services:
  backend:
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

### 定期维护任务

| 任务 | 频率 | 命令 |
|------|------|------|
| 检查服务状态 | 每日 | `docker compose ps` |
| 检查日志错误 | 每日 | `docker compose logs \| grep -i error` |
| 清理无用镜像 | 每周 | `docker image prune -a` |
| 备份配置 | 每周 | `cp .env.docker .env.docker.backup` |
| 检查磁盘空间 | 每月 | `docker system df` |

---

## 数据源配置指南

### 概述

在前端界面配置数据源时，需要正确填写数据库地址。由于后端运行在 Docker 容器中，数据库地址的配置与直接在宿主机上运行应用有所不同。本节将详细介绍如何确认正确的数据库连接地址。

### 1. 确认后端容器运行状态

**步骤 1.1: 检查容器是否正常运行**

```bash
# 查看容器状态
docker compose ps

# 预期输出 (状态应为 "running" 或 "healthy")
NAME                   STATUS
dataprocess_backend    Up 5 minutes (healthy)
dataprocess_frontend   Up 5 minutes
```

**步骤 1.2: 检查容器健康状态**

```bash
# 检查后端服务健康状态
curl http://localhost:8005/health

# 预期输出
{"status":"healthy","service":"data-preprocessing-api"}
```

**步骤 1.3: 查看容器详细信息**

```bash
# 查看容器详细信息
docker inspect dataprocess_backend --format='{{.State.Status}}'

# 查看容器启动时间
docker inspect dataprocess_backend --format='{{.State.StartedAt}}'
```

### 2. 获取容器网络配置信息

**步骤 2.1: 查看容器网络模式**

```bash
# 查看网络模式
docker inspect dataprocess_backend --format='{{.HostConfig.NetworkMode}}'

# 可能的输出:
# - "bridge"    : 默认桥接模式
# - "host"      : 主机模式
# - "default"   : 默认网络
```

**步骤 2.2: 查看网络详细信息**

```bash
# 查看容器网络配置
docker inspect dataprocess_backend --format='{{json .NetworkSettings.Networks}}' | python -m json.tool

# 预期输出示例:
{
  "dataprocess_network": {
    "IPAddress": "172.22.0.2",
    "Gateway": "172.22.0.1",
    "NetworkID": "abc123..."
  }
}
```

**步骤 2.3: 查看网络列表**

```bash
# 列出所有 Docker 网络
docker network ls

# 查看特定网络详情
docker network inspect data_pre_process_dataprocess_network
```

### 3. 识别宿主机 IP 地址

#### 方法一: 查看容器网关地址 (推荐)

```bash
# 获取网关地址 (即宿主机在容器网络中的 IP)
docker inspect dataprocess_backend --format='{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}'

# 输出示例: 172.22.0.1
```

#### 方法二: 使用 host.docker.internal

```bash
# Docker Desktop (Windows/macOS) 提供的特殊 DNS 名称
# 在容器内直接使用 host.docker.internal 访问宿主机

# 验证是否可用
docker exec dataprocess_backend ping -c 2 host.docker.internal
```

#### 方法三: 查看宿主机网络接口

**Linux 系统:**
```bash
# 查看网络接口
ip addr show

# 或使用 ifconfig
ifconfig

# 查看 Docker 网桥地址
ip addr show docker0
```

**Windows 系统 (PowerShell):**
```powershell
# 查看 IP 配置
ipconfig /all

# 查看特定网络适配器
Get-NetIPAddress -InterfaceAlias "vEthernet (WSL)"
```

**macOS 系统:**
```bash
# 查看网络接口
ifconfig

# 查看 en0 接口 (通常是主网络接口)
ifconfig en0
```

#### 方法四: 通过容器环境变量确认

```bash
# 查看容器内的环境变量
docker exec dataprocess_backend env | grep -E "HOST|MYSQL|CK_|MINIO"

# 输出示例:
MYSQL_HOST=host.docker.internal
CK_HOST=host.docker.internal
MINIO_ENDPOINT=http://host.docker.internal:9000
```

### 4. 验证数据库地址连通性

#### 步骤 4.1: 使用健康检查 API

```bash
# 检查所有数据库连接状态
curl http://localhost:8005/health/db | python -m json.tool

# 预期输出:
{
  "status": "healthy",
  "connections": {
    "mysql": {"status": "reachable", "host": "host.docker.internal", "port": 3306},
    "clickhouse": {"status": "reachable", "host": "host.docker.internal", "port": 9006},
    "minio": {"status": "reachable", "endpoint": "http://host.docker.internal:9000"}
  }
}
```

#### 步骤 4.2: 进入容器测试端口连通性

```bash
# 进入后端容器
docker exec -it dataprocess_backend bash

# 测试 MySQL 端口
nc -zv host.docker.internal 3306
# 成功输出: host.docker.internal (172.22.0.1) 3306 (mysql) open

# 测试 ClickHouse 端口
nc -zv host.docker.internal 9006

# 测试 MinIO 端口
nc -zv host.docker.internal 9000

# 退出容器
exit
```

#### 步骤 4.3: 使用 Python 测试数据库连接

```bash
# 测试 MySQL 连接
docker exec dataprocess_backend python -c "
import pymysql
try:
    conn = pymysql.connect(
        host='host.docker.internal',
        port=3306,
        user='root',
        password='infini_rag_flow',
        connect_timeout=5
    )
    print('MySQL 连接成功!')
    conn.close()
except Exception as e:
    print(f'MySQL 连接失败: {e}')
"

# 测试 ClickHouse 连接
docker exec dataprocess_backend python -c "
from clickhouse_driver import Client
try:
    client = Client(host='host.docker.internal', port=9006, connect_timeout=5)
    result = client.execute('SELECT 1')
    print('ClickHouse 连接成功!')
except Exception as e:
    print(f'ClickHouse 连接失败: {e}')
"
```

### 5. 不同网络环境下的配置方式

#### 5.1 桥接模式 (Bridge Mode) - 默认模式

**特点**: 容器使用独立网络，通过网关访问宿主机

**配置方式**:
```yaml
# docker-compose.yml
services:
  backend:
    networks:
      - dataprocess_network
    extra_hosts:
      - "host.docker.internal:host-gateway"

networks:
  dataprocess_network:
    driver: bridge
```

**数据源地址配置**:
| 数据库 | 地址填写 |
|--------|---------|
| MySQL | `host.docker.internal` 或 `172.22.0.1` |
| ClickHouse | `host.docker.internal` 或 `172.22.0.1` |
| MinIO | `http://host.docker.internal:9000` |

#### 5.2 主机模式 (Host Mode)

**特点**: 容器直接使用宿主机网络，无网络隔离

**配置方式**:
```yaml
# docker-compose.yml
services:
  backend:
    network_mode: "host"
```

**数据源地址配置**:
| 数据库 | 地址填写 |
|--------|---------|
| MySQL | `localhost` 或 `127.0.0.1` |
| ClickHouse | `localhost` 或 `127.0.0.1` |
| MinIO | `http://localhost:9000` |

**注意**: 主机模式下无需端口映射，服务直接监听宿主机端口。

#### 5.3 自定义网络模式

**特点**: 多个容器共享同一网络，可通过容器名互相访问

**配置方式**:
```yaml
# docker-compose.yml
services:
  backend:
    networks:
      - app_network
  mysql:
    image: mysql:8.0
    networks:
      - app_network

networks:
  app_network:
    driver: bridge
```

**数据源地址配置**:
| 数据库 | 地址填写 |
|--------|---------|
| MySQL (容器内) | `mysql` (容器服务名) |
| 外部 MySQL | `host.docker.internal` |

### 6. 前端界面配置数据源步骤

#### 步骤 6.1: 打开数据源管理页面

1. 访问前端界面: http://localhost:13005
2. 导航至 "数据源管理" 页面
3. 点击 "新建数据源"

#### 步骤 6.2: 填写数据源信息

**MySQL 数据源配置示例**:

| 字段 | 填写值 | 说明 |
|------|--------|------|
| 类型 | MySQL | 选择数据库类型 |
| 名称 | my_mysql | 自定义名称 |
| 主机地址 | `host.docker.internal` | 或网关 IP |
| 端口 | `3306` | MySQL 默认端口 |
| 用户名 | `root` | 数据库用户 |
| 密码 | `your_password` | 数据库密码 |
| 数据库 | `test_db` | 数据库名称 |

**ClickHouse 数据源配置示例**:

| 字段 | 填写值 | 说明 |
|------|--------|------|
| 类型 | ClickHouse | 选择数据库类型 |
| 名称 | my_clickhouse | 自定义名称 |
| 主机地址 | `host.docker.internal` | 或网关 IP |
| 端口 | `9006` | ClickHouse 端口 |
| 用户名 | `admin` | 数据库用户 |
| 密码 | `your_password` | 数据库密码 |

#### 步骤 6.3: 测试连接

1. 填写完配置后，点击 "测试连接" 按钮
2. 查看连接结果提示
3. 如果连接失败，根据错误信息排查

### 7. 常见问题排查

#### 问题 1: host.docker.internal 无法解析

**症状**: 连接失败，提示 "name resolution failed"

**原因**: 某些 Linux 发行版不支持 `host.docker.internal`

**解决方案**:
```yaml
# 在 docker-compose.yml 中添加
services:
  backend:
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

或使用网关 IP:
```bash
# 获取网关 IP
GATEWAY_IP=$(docker inspect dataprocess_backend --format='{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}')
echo "网关 IP: $GATEWAY_IP"
```

#### 问题 2: 端口无法连接

**症状**: 连接超时或拒绝

**排查步骤**:
```bash
# 1. 确认数据库服务已启动
# MySQL
systemctl status mysql
# 或
docker ps | grep mysql

# 2. 确认端口监听
netstat -tlnp | grep 3306

# 3. 检查防火墙
sudo firewall-cmd --list-ports
# 或
sudo iptables -L -n | grep 3306

# 4. 从宿主机测试连接
mysql -h localhost -u root -p
```

**解决方案**:
- 启动数据库服务
- 开放防火墙端口
- 检查数据库配置中的 bind-address

#### 问题 3: 认证失败

**症状**: 连接成功但认证失败

**排查步骤**:
```bash
# 检查用户权限 (MySQL)
mysql -u root -p
> SELECT user, host FROM mysql.user;

# 检查用户是否有远程访问权限
> SHOW GRANTS FOR 'root'@'%';

# 如果没有，创建远程访问权限
> CREATE USER 'root'@'%' IDENTIFIED BY 'password';
> GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';
> FLUSH PRIVILEGES;
```

#### 问题 4: 网络模式不匹配

**症状**: 配置正确但无法连接

**排查步骤**:
```bash
# 检查当前网络模式
docker inspect dataprocess_backend --format='{{.HostConfig.NetworkMode}}'

# 如果是 host 模式，应使用 localhost
# 如果是 bridge 模式，应使用 host.docker.internal
```

### 8. 配置速查表

| 网络模式 | 数据库地址 | 说明 |
|---------|-----------|------|
| bridge | `host.docker.internal` | Docker Desktop 默认支持 |
| bridge | `172.22.0.1` (网关 IP) | Linux 系统通用 |
| host | `localhost` 或 `127.0.0.1` | 容器共享宿主机网络 |
| 自定义网络 | 容器服务名 | 同一网络内的容器互联 |

---

## 数据备份与恢复

### 配置文件备份

```bash
# 备份环境变量配置
cp .env.docker .env.docker.backup.$(date +%Y%m%d)

# 备份 docker-compose.yml
cp docker-compose.yml docker-compose.yml.backup.$(date +%Y%m%d)
```

### 镜像备份

```bash
# 导出镜像
docker save data_pre_process-backend:latest -o backend_image.tar
docker save data_pre_process-frontend:latest -o frontend_image.tar

# 导入镜像
docker load -i backend_image.tar
docker load -i frontend_image.tar
```

### 完整备份脚本

```bash
#!/bin/bash
# backup.sh - 完整备份脚本

BACKUP_DIR="/backup/$(date +%Y%m%d_%H%M%S)"
mkdir -p $BACKUP_DIR

# 备份配置文件
cp .env.docker $BACKUP_DIR/
cp docker-compose.yml $BACKUP_DIR/

# 导出镜像
docker save data_pre_process-backend:latest -o $BACKUP_DIR/backend_image.tar
docker save data_pre_process-frontend:latest -o $BACKUP_DIR/frontend_image.tar

# 备份数据库 (示例)
# mysqldump -h host.docker.internal -u root -p test_db > $BACKUP_DIR/mysql_backup.sql

echo "Backup completed: $BACKUP_DIR"
```

### 恢复操作

```bash
# 1. 停止服务
docker compose down

# 2. 恢复配置文件
cp /backup/20260316_120000/.env.docker .
cp /backup/20260316_120000/docker-compose.yml .

# 3. 导入镜像
docker load -i /backup/20260316_120000/backend_image.tar
docker load -i /backup/20260316_120000/frontend_image.tar

# 4. 启动服务
docker compose up -d

# 5. 验证服务
curl http://localhost:8005/health
```

---

## 故障排除

### 故障诊断流程

```
┌─────────────────┐
│   服务异常?      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 检查容器状态     │ ── docker compose ps
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 查看容器日志     │ ── docker compose logs -f
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 检查网络连接     │ ── curl http://localhost:8005/health
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 检查依赖服务     │ ── curl http://localhost:8005/health/db
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 重启服务         │ ── docker compose restart
└─────────────────┘
```

### 常见故障处理

#### 1. 服务无法访问

**症状**: 浏览器无法打开前端页面

**排查步骤**:
```bash
# 1. 检查容器状态
docker compose ps

# 2. 检查端口占用
netstat -tlnp | grep -E "8005|13005"

# 3. 检查防火墙
sudo firewall-cmd --list-ports

# 4. 检查容器日志
docker compose logs frontend
```

**解决方案**:
- 重启服务: `docker compose restart`
- 检查端口映射是否正确
- 确认防火墙已开放端口

#### 2. API 请求超时

**症状**: 前端请求后端 API 超时

**排查步骤**:
```bash
# 1. 检查后端服务
curl http://localhost:8005/health

# 2. 检查数据库连接
curl http://localhost:8005/health/db

# 3. 查看后端日志
docker compose logs backend --tail 100
```

**解决方案**:
- 检查数据库服务是否正常
- 检查网络连接
- 重启后端服务

#### 3. 数据库连接失败

**症状**: 数据库操作报错

**排查步骤**:
```bash
# 1. 检查数据库连接状态
curl http://localhost:8005/health/db

# 2. 进入容器测试连接
docker exec -it dataprocess_backend bash
nc -zv host.docker.internal 3306
nc -zv host.docker.internal 9006

# 3. 检查环境变量
docker exec dataprocess_backend env | grep -E "MYSQL|CK_"
```

**解决方案**:
- 确认数据库服务已启动
- 检查环境变量配置
- 验证数据库用户权限

#### 4. 容器内存不足

**症状**: 容器频繁重启，OOM 错误

**排查步骤**:
```bash
# 查看容器资源使用
docker stats --no-stream

# 查看容器事件
docker events --filter 'container=dataprocess_backend'
```

**解决方案**:
```yaml
# 在 docker-compose.yml 中增加内存限制
services:
  backend:
    deploy:
      resources:
        limits:
          memory: 2G
```

### 紧急恢复流程

```bash
# 1. 停止所有服务
docker compose down

# 2. 清理资源
docker system prune -f

# 3. 重新构建
docker compose build --no-cache

# 4. 启动服务
docker compose up -d

# 5. 验证服务
curl http://localhost:8005/health
```

---

## 附录

### A. 环境变量完整列表

| 变量名 | 说明 | 示例值 |
|--------|------|--------|
| `MYSQL_HOST` | MySQL 主机地址 | `host.docker.internal` |
| `MYSQL_PORT` | MySQL 端口 | `3306` |
| `MYSQL_USER` | MySQL 用户名 | `root` |
| `MYSQL_PASSWORD` | MySQL 密码 | `your_password` |
| `MYSQL_DB` | MySQL 数据库名 | `test_db` |
| `CK_HOST` | ClickHouse 主机 | `host.docker.internal` |
| `CK_PORT` | ClickHouse 端口 | `9006` |
| `CK_USER` | ClickHouse 用户名 | `admin` |
| `CK_PASSWORD` | ClickHouse 密码 | `your_password` |
| `MINIO_ENDPOINT` | MinIO 端点 | `http://host.docker.internal:9000` |
| `MINIO_ROOT_USER` | MinIO 用户名 | `minioadmin` |
| `MINIO_ROOT_PASSWORD` | MinIO 密码 | `your_password` |
| `BACKEND_SERVICE_URL` | 后端服务地址 | `http://localhost:8005` |

### B. 常用端口列表

| 服务 | 端口 | 协议 |
|------|------|------|
| 前端服务 | 13005 | HTTP |
| 后端 API | 8005 | HTTP |
| MySQL | 3306 | TCP |
| ClickHouse | 9006 | TCP |
| ClickHouse HTTP | 8123 | HTTP |
| MinIO API | 9000 | HTTP |
| MinIO Console | 9001 | HTTP |

### C. 快捷命令别名

添加到 `~/.bashrc` 或 `~/.zshrc`:

```bash
# 数据预处理系统快捷命令
alias dps-up='docker compose up -d'
alias dps-down='docker compose down'
alias dps-logs='docker compose logs -f'
alias dps-ps='docker compose ps'
alias dps-health='curl -s http://localhost:8005/health | python -m json.tool'
alias dps-db='curl -s http://localhost:8005/health/db | python -m json.tool'
alias dps-restart='docker compose restart'
alias dps-shell='docker exec -it dataprocess_backend bash'
```

### D. 联系支持

遇到无法解决的问题时，请收集以下信息并联系技术支持：

1. **系统信息**
   ```bash
   docker info > system_info.txt
   docker compose ps >> system_info.txt
   ```

2. **日志信息**
   ```bash
   docker compose logs --tail 500 > logs.txt
   ```

3. **健康状态**
   ```bash
   curl http://localhost:8005/health > health.txt
   curl http://localhost:8005/health/db >> health.txt
   ```

4. **错误描述**
   - 问题发生时间
   - 操作步骤
   - 错误信息截图
   - 预期结果 vs 实际结果
