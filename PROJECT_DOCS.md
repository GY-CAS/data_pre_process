# 项目代码梳理与功能架构文档

## 1. 模块与 API 接口梳理

本项目后端基于 FastAPI 构建，主要分为四个核心功能模块：数据引接、数据同步、数据管理、行为预警。

### 1.1 数据引接 (Data Ingestion)
负责管理外部数据源的连接配置与连通性测试。
- **对应文件**: `backend/app/api/datasource.py`
- **接口列表**:
    - `POST /datasources/`: **创建数据源**
        - 功能: 注册新的 MySQL、ClickHouse 或 MinIO 数据源。包含重名检测逻辑。
    - `GET /datasources/`: **获取数据源列表**
        - 功能: 分页查询数据源，支持按名称和类型筛选。
    - `GET /datasources/{datasource_id}`: **获取数据源详情**
        - 功能: 获取指定 ID 的数据源详细配置。
    - `DELETE /datasources/{datasource_id}`: **删除数据源**
        - 功能: 移除数据源配置。
    - `GET /datasources/{datasource_id}/metadata`: **获取元数据**
        - 功能: 获取数据源中的表名列表或存储桶列表。
    - `POST /datasources/test-connection`: **测试连接**
        - 功能: 验证提供的连接信息是否能成功连接到目标数据库或存储服务。

### 1.2 数据同步 (Data Synchronization)
负责定义和执行数据迁移任务，支持全量同步与预处理。
- **对应文件**: `backend/app/api/task.py`
- **接口列表**:
    - `POST /tasks/`: **创建任务**
        - 功能: 定义新的同步或计算任务（包含源、目标及配置）。
    - `GET /tasks/`: **获取任务列表**
        - 功能: 分页查询任务状态与详情。
    - `GET /tasks/{task_id}`: **获取任务详情**
        - 功能: 获取特定任务的配置与执行历史信息。
    - `POST /tasks/{task_id}/run`: **运行任务**
        - 功能: 触发后台异步任务（`run_sync_task` 或 `run_spark_job_background`），执行数据同步。
    - `DELETE /tasks/{task_id}`: **删除任务**
        - 功能: 删除单个任务。
    - `DELETE /tasks/`: **批量删除任务**
        - 功能: 根据 ID 列表批量删除任务。

### 1.3 数据管理 (Data Management)
负责对已同步到系统内部或目标库的数据资产进行预览、编辑与维护。
- **对应文件**: `backend/app/api/data_management.py`
- **接口列表**:
    - `GET /data-mgmt/assets`: **获取数据资产列表**
        - 功能: 列出所有已同步的表（MySQL/ClickHouse）和存储桶（MinIO），以及本地文件。
    - `GET /data-mgmt/preview`: **预览数据**
        - 功能: 分页查看表数据或 MinIO 对象列表。
    - `GET /data-mgmt/structure`: **查看结构**
        - 功能: 获取表的列结构（字段名、类型）或 MinIO 对象的元数据结构。
    - `DELETE /data-mgmt/{name_or_id}`: **删除资产**
        - 功能: 从注册表中移除资产，并物理删除对应的数据库表或 MinIO 存储桶/对象。
    - `GET /data-mgmt/download/{name_or_id}`: **下载/导出资产**
        - 功能: 将表数据导出为 CSV/Excel/JSON，或生成 MinIO 文件的预签名下载链接。
    - `PUT /data-mgmt/table/{table_name}/row/{row_id}`: **更新行数据**
        - 功能: 修改系统库中表的指定行数据。
    - `DELETE /data-mgmt/table/{table_name}/row/{row_id}`: **删除行数据**
        - 功能: 删除系统库中表的指定行。

### 1.4 行为预警 (Behavior Alerting)
负责记录系统操作日志及数据校验异常，提供审计与追溯能力。
- **对应文件**: `backend/app/api/audit.py`
- **接口列表**:
    - `GET /audit/`: **获取审计日志**
        - 功能: 分页查询系统日志，支持按用户、操作类型、资源对象筛选。包含任务执行结果及数据校验失败信息。
    - `POST /audit/`: **创建日志**
        - 功能: 手动记录审计日志（通常由系统内部调用）。
    - `DELETE /audit/`: **批量删除日志**
        - 功能: 清理审计记录。

---

## 2. 数据库表梳理

系统使用 SQLModel (基于 SQLAlchemy) 定义数据模型，主要包含以下四张表：

### 2.1 DataSource (数据源配置)
- **表名**: `datasource`
- **用途**: 存储外部数据源的连接信息。
- **字段**:
    - `id` (int, PK): 主键。
    - `name` (str): 数据源名称。
    - `description` (str): 描述信息。
    - `type` (str): 类型 (mysql, clickhouse, minio)。
    - `connection_info` (json str): 连接凭证（Host, Port, User, Password 等）。
    - `created_at`, `updated_at`: 时间戳。

### 2.2 DataTask (数据任务)
- **表名**: `datatask`
- **用途**: 定义数据同步或处理任务及其当前状态。
- **字段**:
    - `id` (int, PK): 主键。
    - `name` (str): 任务名称。
    - `task_type` (str): 任务类型 (full_sync, preprocess)。
    - `config` (json str): 任务配置（源ID、目标表名、同步模式等）。
    - `status` (str): 状态 (pending, running, success, failed)。
    - `verification_status` (str): 数据校验状态 (verified, failed)。
    - `progress` (int): 进度百分比 (0-100)。
    - `created_at`, `updated_at`: 时间戳。

### 2.3 SyncedTable (已同步资产注册表)
- **表名**: `syncedtable`
- **用途**: 记录同步任务成功后生成的资产，用于在“数据管理”模块中展示。
- **字段**:
    - `id` (int, PK): 主键。
    - `table_name` (str): 目标表名或存储桶名。
    - `source_type` (str): 数据来源类型 (mysql, clickhouse, minio)。
    - `source_name` (str): 来源数据源名称。
    - `row_count` (int): 同步时的行数记录。
    - `created_at`, `updated_at`: 时间戳。

### 2.4 AuditLog (审计与预警日志)
- **表名**: `auditlog`
- **用途**: 记录用户操作（如创建/删除）及系统事件（如任务失败、校验不一致）。
- **字段**:
    - `id` (int, PK): 主键。
    - `user_id` (str): 操作用户（或 system）。
    - `action` (str): 动作类型 (create_datasource, verification_failed 等)。
    - `resource` (str): 受影响的资源名称。
    - `details` (str): 详细信息（如错误堆栈、校验差异详情）。
    - `timestamp`: 发生时间。

---

## 3. 功能架构图

```mermaid
graph TD
    %% 前端层
    subgraph Frontend [前端应用 (React)]
        PageDS[数据源管理页面]
        PageTask[任务管理页面]
        PageData[数据管理页面]
        PageAudit[行为预警/日志页面]
    end

    %% API 网关层
    subgraph Backend [后端服务 (FastAPI)]
        RouterDS[Data Source API]
        RouterTask[Task API]
        RouterData[Data Mgmt API]
        RouterAudit[Audit API]
        
        ServiceSync[Sync Service (同步引擎)]
        ServiceSpark[Spark Service (计算引擎)]
    end

    %% 存储层
    subgraph Storage [数据存储]
        MetaDB[(Metadata DB - SQLite/MySQL)]
        SystemDB[(System DB - MySQL)]
    end

    %% 外部资源
    subgraph External [外部数据源]
        ExtMySQL[(MySQL)]
        ExtCK[(ClickHouse)]
        ExtMinIO[(MinIO / S3)]
    end

    %% 交互关系 - 数据源管理
    PageDS -->|HTTP| RouterDS
    RouterDS -->|CRUD| MetaDB
    RouterDS -->|Test/Metadata| ExtMySQL
    RouterDS -->|Test/Metadata| ExtCK
    RouterDS -->|Test/Metadata| ExtMinIO

    %% 交互关系 - 任务管理
    PageTask -->|HTTP| RouterTask
    RouterTask -->|CRUD| MetaDB
    RouterTask -->|Trigger| ServiceSync
    RouterTask -->|Trigger| ServiceSpark

    %% 交互关系 - 同步服务
    ServiceSync -->|Read| ExtMySQL
    ServiceSync -->|Read| ExtCK
    ServiceSync -->|Read| ExtMinIO
    ServiceSync -->|Write| SystemDB
    ServiceSync -->|Write| ExtCK
    ServiceSync -->|Write| ExtMinIO
    ServiceSync -->|Register Asset| MetaDB
    ServiceSync -->|Log Verification| RouterAudit

    %% 交互关系 - 数据管理
    PageData -->|HTTP| RouterData
    RouterData -->|Query Registry| MetaDB
    RouterData -->|Preview/Edit| SystemDB
    RouterData -->|Preview| ExtCK
    RouterData -->|Preview/Presign| ExtMinIO

    %% 交互关系 - 审计
    PageAudit -->|HTTP| RouterAudit
    RouterAudit -->|Read/Write| MetaDB
    RouterDS -.->|Log| RouterAudit
    RouterTask -.->|Log| RouterAudit
    RouterData -.->|Log| RouterAudit
```
