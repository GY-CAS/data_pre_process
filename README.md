# 数据预处理分系统 (Data Preprocessing System)

本项目是一个现代化的数据预处理与同步平台，旨在提供高效的数据引接、清洗、转换和管理功能。系统采用前后端分离架构，前端基于 React 和 Tailwind CSS 构建现代化 UI，后端基于 FastAPI 和 PySpark 提供高性能的数据处理能力。

## 🌟 核心功能

*   **🔌 数据引接 (Data Sources)**
    *   支持多种数据源连接：MySQL 数据库、MinIO 对象存储、本地 CSV 文件。
    *   提供数据源的连接测试和元数据查看功能。

*   **🔄 数据同步与任务管理 (Tasks)**
    *   **全量同步**：支持从源（MySQL/MinIO）到目标（系统库/MinIO）的数据全量同步，支持追加 (Append) 和覆盖 (Overwrite) 模式。
    *   **预处理任务**：集成 PySpark 引擎，支持去重 (Dedup)、缺失值处理等数据清洗操作。
    *   **任务监控**：实时查看任务状态、进度和失败原因。

*   **📂 数据管理 (Data Management)**
    *   统一管理本地文件和数据库表资产。
    *   支持数据预览、结构查看、行级编辑与删除。
    *   提供分页查询和批量操作功能。

*   **🛡️ 行为预警 (Audit Logs)**
    *   记录系统关键操作和异常日志。
    *   支持按用户和行为筛选日志，提供批量删除功能。

## 🛠️ 技术栈

### 后端 (Backend)
*   **框架**: [FastAPI](https://fastapi.tiangolo.com/)
*   **ORM**: [SQLModel](https://sqlmodel.tiangolo.com/) (SQLAlchemy + Pydantic)
*   **数据处理**: [PySpark](https://spark.apache.org/docs/latest/api/python/), [Pandas](https://pandas.pydata.org/)
*   **数据库**: MySQL (主要存储), SQLite (可选)
*   **服务**: Uvicorn

### 前端 (Frontend)
*   **框架**: [React 19](https://react.dev/)
*   **构建工具**: [Vite](https://vitejs.dev/)
*   **样式**: [Tailwind CSS](https://tailwindcss.com/)
*   **图标**: [Lucide React](https://lucide.dev/)
*   **HTTP 客户端**: Axios

## 🚀 快速开始

### 前置要求
*   Python 3.8+
*   Node.js 16+
*   MySQL 数据库
*   MinIO (可选，用于对象存储功能)
*   Java (用于 PySpark 运行环境)

### 1. 环境配置

在项目根目录创建 `.env` 文件，配置数据库连接信息：

```ini
# MySQL 配置
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DB=test_db

# MinIO 配置 (如需使用 MinIO 功能)
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

### 2. 后端启动

安装依赖并启动 API 服务：

```bash
# 进入项目根目录
pip install -r backend/requirements.txt

# 启动后端服务 (默认端口 8000)
python -m uvicorn backend.app.main:app --reload
```

后端 API 文档地址: http://127.0.0.1:8000/docs

### 3. 前端启动

安装依赖并启动开发服务器：

```bash
# 进入前端目录
cd frontend

# 安装依赖
npm install

# 启动开发服务 (默认端口 3000)
npm run dev
```

访问前端页面: http://localhost:3000/

## 📂 项目结构

```
data_pre_process/
├── backend/                # 后端代码
│   ├── app/
│   │   ├── api/            # API 路由接口
│   │   ├── core/           # 核心配置与数据库连接
│   │   ├── models/         # 数据库模型 (SQLModel)
│   │   └── services/       # 业务逻辑服务 (Sync, Spark)
│   ├── operators/          # 数据处理算子
│   ├── spark_jobs/         # Spark 任务脚本
│   └── requirements.txt    # Python 依赖
├── frontend/               # 前端代码
│   ├── src/
│   │   ├── components/     # 通用组件
│   │   ├── pages/          # 页面组件 (Tasks, DataSources, etc.)
│   │   └── api.js          # API 接口定义
│   └── package.json        # Node.js 依赖
├── data/                   # 本地数据存储目录
├── .env                    # 环境变量配置文件
└── README.md               # 项目说明文档
```

## ✨ 页面展示

*   **数据引接**: 统一管理各类数据源连接。
*   **数据同步**: 创建和管理数据同步与处理任务。
*   **数据管理**: 可视化预览和编辑数据内容。
*   **行为预警**: 监控系统运行状态与操作审计。

---
© 2026 Data Preprocessing System. All Rights Reserved.
