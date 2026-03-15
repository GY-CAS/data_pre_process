# Data Preprocessing System API 接口文档

## 概述

Data Preprocessing System API 是一个用于数据预处理任务的RESTful API服务，基于 FastAPI 构建。

**基础URL**: `http://localhost:8005`

**API前缀**: `/api/v1` (如需配置，可通过环境变量 `API_V1_STR` 修改)

---

## 目录

1. [数据源管理 (DataSources)](#1-数据源管理-datasources)
2. [任务管理 (Tasks)](#2-任务管理-tasks)
3. [审计日志 (Audit)](#3-审计日志-audit)
4. [数据管理 (Data Management)](#4-数据管理-data-management)

---

## 1. 数据源管理 (DataSources)

### 1.1 创建数据源

创建新的数据源连接配置。

**URL**: `POST /datasources/`

**请求头**:
| Header | 值 | 必填 | 说明 |
|--------|-----|------|------|
| Content-Type | application/json | 是 | 请求体格式 |

**请求体 (Request Body)**:

```json
{
  "name": "string",
  "type": "string",
  "description": "string",
  "data_type  "connection_info": "string",
": "string"
}
```

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| name | string | 是 | - | 数据源名称 |
| type | string | 是 | - | 数据源类型：`mysql`, `clickhouse`, `minio`, `csv` |
| description | string | 否 | null | 数据源描述 |
| data_type | string | 否 | null | 数据类型：`IMAGE`, `TIMESERIES`, `NER` |
| connection_info | string | 是 | - | 连接信息JSON字符串 |

**connection_info 格式示例**:

*MySQL*:
```json
{
  "host": "localhost",
  "port": 3306,
  "user": "root",
  "password": "password",
  "database": "test_db"
}
```

*ClickHouse*:
```json
{
  "host": "localhost",
  "port": 9000,
  "user": "default",
  "password": "default",
  "database": "default"
}
```

*MinIO*:
```json
{
  "endpoint": "http://localhost:9000",
  "access_key": "minioadmin",
  "secret_key": "minioadmin"
}
```

*CSV文件*:
```json
{
  "path": "/path/to/file.csv"
}
```

**响应 (201 Created)**:

```json
{
  "id": 1,
  "name": "my_mysql",
  "type": "mysql",
  "description": "MySQL数据源",
  "data_type": null,
  "connection_info": "{\"host\": \"localhost\", ...}",
  "created_at": "2024-01-01T00:00:00",
  "updated_at": "2024-01-01T00:00:00"
}
```

**错误响应 (400)**:
```json
{
  "detail": "Data source with name 'xxx' already exists for type 'yyy'"
}
```

---

### 1.2 获取数据源列表

获取所有数据源，支持分页和过滤。

**URL**: `GET /datasources/`

**查询参数 (Query Parameters)**:

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| skip | integer | 否 | 0 | 跳过记录数（分页偏移） |
| limit | integer |否 | 100 | 返回记录数（分页大小） |
| name | string | 否 | null | 按名称模糊搜索 |
| type | string | 否 | null | 按类型精确过滤 |

**响应 (200 OK)**:

```json
{
  "data": [
    {
      "id": 1,
      "name": "my_mysql",
      "type": "mysql",
      "description": "MySQL数据源",
      "data_type": null,
      "connection_info": "...",
      "created_at": "2024-01-01T00:00:00",
      "updated_at": "2024-01-01T00:00:00"
    }
  ],
  "total": 10,
  "skip": 0,
  "limit": 100
}
```

---

### 1.3 获取单个数据源

根据ID获取指定数据源的详细信息。

**URL**: `GET /datasources/{datasource_id}`

**路径参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| datasource_id | integer | 是 | 数据源ID |

**响应 (200 OK)**:

```json
{
  "id": 1,
  "name": "my_mysql",
  "type": "mysql",
  "description": "MySQL数据源",
  "data_type": null,
  "connection_info": "...",
  "created_at": "2024-01-01T00:00:00",
  "updated_at": "2024-01-01T00:00:00"
}
```

**错误响应 (404)**:
```json
{
  "detail": "DataSource not found"
}
```

---

### 1.4 删除数据源

删除指定的数据源。

**URL**: `DELETE /datasources/{datasource_id}`

**路径参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| datasource_id | integer | 是 | 数据源ID |

**响应 (200 OK)**:

```json
{
  "ok": true
}
```

**错误响应 (404)**:
```json
{
  "detail": "DataSource not found"
}
```

---

### 1.5 获取数据源元数据

获取数据源中的表/桶列表。

**URL**: `GET /datasources/{datasource_id}/metadata`

**路径参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| datasource_id | integer | 是 | 数据源ID |

**响应 (200 OK)**:

*MySQL/ClickHouse*:
```json
{
  "tables": ["table1", "table2", "table3"]
}
```

*MinIO*:
```json
{
  "tables": ["bucket1", "bucket2"]
}
```

**错误响应 (400)**:
```json
{
  "detail": "Failed to fetch metadata: {error_message}"
}
```

---

### 1.6 测试数据源连接

测试数据源连接是否可用。

**URL**: `POST /datasources/test-connection`

**请求体 (Request Body)**:

```json
{
  "type": "mysql",
  "host": "localhost",
  "port": 3306,
  "user": "root",
  "password": "password",
  "database": "test_db"
}
```

**响应 (200 OK)**:

*成功*:
```json
{
  "status": "success",
  "message": "Successfully connected to MySQL"
}
```

*失败*:
```json
{
  "status": "error",
  "message": "Connection failed: {error_message}"
}
```

---

## 2. 任务管理 (Tasks)

### 2.1 创建任务

创建一个新的数据处理任务。

**URL**: `POST /tasks/`

**请求体 (Request Body)**:

```json
{
  "name": "string",
  "task_type": "string",
  "config": "string"
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name | string | 是 | 任务名称 |
| task_type | string | 是 | 任务类型：`full_sync`(全量同步), `preprocess`(预处理), `sync`(数据同步) |
| config | string | 是 | 任务配置JSON字符串，包含source、target、operators等 |

**config 格式示例**:

```json
{
  "source": {
    "type": "mysql",
    "datasource_id": 1,
    "table": "source_table"
  },
  "target": {
    "type": "clickhouse",
    "table": "target_table"
  },
  "operators": [
    {"type": "missing", "columns": ["col1", "col2"]},
    {"type": "outlier", "columns": ["col3"]}
  ]
}
```

**响应 (201 Created)**:

```json
{
  "id": 1,
  "name": "my_task",
  "task_type": "preprocess",
  "config": "{\"source\": {...}}",
  "status": "pending",
  "verification_status": null,
  "progress": 0,
  "spark_app_id": null,
  "created_at": "2024-01-01T00:00:00",
  "updated_at": null
}
```

---

### 2.2 获取任务列表

获取所有任务，支持分页和名称过滤。

**URL**: `GET /tasks/`

**查询参数**:

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| skip | integer | 否 | 0 | 分页偏移 |
| limit | integer | 否 | 100 | 分页大小 |
| name | string | 否 | null | 按名称模糊搜索 |

**响应 (200 OK)**:

```json
{
  "items": [
    {
      "id": 1,
      "name": "my_task",
      "task_type": "preprocess",
      "config": "...",
      "status": "success",
      "verification_status": "verified",
      "progress": 100,
      "spark_app_id": "app-123",
      "created_at": "2024-01-01T00:00:00",
      "updated_at": "2024-01-01T01:00:00"
    }
  ],
  "total": 5
}
```

**任务状态说明**:

| status | 说明 |
|--------|------|
| pending | 等待执行 |
| running | 执行中 |
| success | 执行成功 |
| failed | 执行失败 |

| verification_status | 说明 |
|---------------------|------|
| verified | 验证通过 |
| failed | 验证失败 |
| null | 未验证 |

---

### 2.3 获取单个任务

根据ID获取任务详情。

**URL**: `GET /tasks/{task_id}`

**路径参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| task_id | integer | 是 | 任务ID |

**响应 (200 OK)**:

```json
{
  "id": 1,
  "name": "my_task",
  "task_type": "preprocess",
  "config": "...",
  "status": "running",
  "verification_status": null,
  "progress": 50,
  "spark_app_id": "app-123",
  "created_at": "2024-01-01T00:00:00",
  "updated_at": "2024-01-01T00:30:00"
}
```

**错误响应 (404)**:
```json
{
  "detail": "Task not found"
}
```

---

### 2.4 执行任务

触发任务执行（异步后台执行）。

**URL**: `POST /tasks/{task_id}/run`

**路径参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| task_id | integer | 是 | 任务ID |

**响应 (200 OK)**:

```json
{
  "message": "Task started",
  "task_id": 1
}
```

**错误响应 (404)**:
```json
{
  "detail": "Task not found"
}
```

---

### 2.5 删除单个任务

删除指定任务。

**URL**: `DELETE /tasks/{task_id}`

**路径参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| task_id | integer | 是 | 任务ID |

**响应 (200 OK)**:

```json
{
  "ok": true
}
```

---

### 2.6 批量删除任务

批量删除多个任务。

**URL**: `DELETE /tasks/`

**请求体 (Request Body)**:

```json
{
  "ids": [1, 2, 3]
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ids | array[integer] | 是 | 任务ID数组 |

**响应 (200 OK)**:

```json
{
  "ok": true,
  "count": 3
}
```

---

## 3. 审计日志 (Audit)

### 3.1 获取审计日志列表

获取系统操作审计日志。

**URL**: `GET /audit/`

**查询参数**:

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| skip | integer | 否 | 0 | 分页偏移 |
| limit | integer | 否 | 100 | 分页大小 |
| user_id | string | 否 | null | 按用户ID过滤 |
| action | string | 否 | null | 按操作类型过滤 |
| resource | string | 否 | null | 按资源名称过滤 |

**响应 (200 OK)**:

```json
{
  "items": [
    {
      "id": 1,
      "user_id": "admin",
      "action": "create_datasource",
      "resource": "my_mysql",
      "details": "Type: mysql, Description: MySQL数据源",
      "timestamp": "2024-01-01T00:00:00"
    }
  ],
  "total": 100
}
```

**操作类型 (action) 说明**:

| action | 说明 |
|--------|------|
| create_datasource | 创建数据源 |
| delete_datasource | 删除数据源 |
| create_task | 创建任务 |
| run_task | 执行任务 |
| task_completed | 任务完成 |
| task_failed | 任务失败 |
| delete_asset | 删除数据资产 |
| update_row | 更新行数据 |
| delete_row | 删除行数据 |
| download_asset | 下载数据资产 |

---

### 3.2 创建审计日志

手动创建审计日志条目。

**URL**: `POST /audit/`

**请求体 (Request Body)**:

```json
{
  "user_id": "string",
  "action": "string",
  "resource": "string",
  "details": "string"
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| user_id | string | 是 | 用户ID |
| action | string | 是 | 操作类型 |
| resource | string | 是 | 资源名称 |
| details | string | 否 | 详细信息 |

**响应 (201 Created)**:

```json
{
  "id": 1,
  "user_id": "admin",
  "action": "custom_action",
  "resource": "resource_name",
  "details": "some details",
  "timestamp": "2024-01-01T00:00:00"
}
```

---

### 3.3 批量删除审计日志

批量删除指定审计日志。

**URL**: `DELETE /audit/`

**请求体 (Request Body)**:

```json
{
  "ids": [1, 2, 3]
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ids | array[integer] | 是 | 审计日志ID数组 |

**响应 (200 OK)**:

```json
{
  "ok": true,
  "count": 3
}
```

---

## 4. 数据管理 (Data Management)

### 4.1 获取数据资产列表

获取所有数据资产（本地文件 + 同步表）。

**URL**: `GET /data-mgmt/assets`

**响应 (200 OK)**:

```json
[
  {
    "id": 1,
    "name": "data.csv",
    "type": "file",
    "path": "data/data.csv",
    "size": "1024.50 KB",
    "source": "Local File",
    "rows": 0,
    "data_type": null
  },
  {
    "id": 2,
    "name": "synced_table",
    "type": "table",
    "path": "synced_table",
    "size": "-",
    "source": "clickhouse",
    "rows": 10000,
    "data_type": "TIMESERIES"
  },
  {
    "id": 3,
    "name": "my_bucket",
    "type": "bucket",
    "path": "my_bucket",
    "size": "-",
    "source": "minio",
    "rows": 50,
    "data_type": "IMAGE"
  }
]
```

**字段说明**:

| 字段 | 类型 | 说明 |
|------|------|------|
| type | string | 资产类型：`file`(本地文件), `table`(数据库表), `bucket`(对象存储桶) |
| source | string | 数据来源：`Local File`, `mysql`, `clickhouse`, `minio` |
| data_type | string | 数据类型：`TIMESERIES`, `IMAGE`, `NER`, null |

---

### 4.2 获取完整数据资产信息

获取所有数据资产的完整信息，包括存储位置等。

**URL**: `GET /data-mgmt/assets-complete`

**响应 (200 OK)**:

```json
[
  {
    "id": 1,
    "name": "synced_table",
    "type": "table",
    "data_type": "TIMESERIES",
    "source": "clickhouse",
    "source_name": "原始数据源名称",
    "row_count": 10000,
    "created_at": "2024-01-01T00:00:00",
    "updated_at": "2024-01-01T00:00:00",
    "storage": {
      "type": "clickhouse",
      "location": "table_name",
      "endpoint": "http://localhost:8123"
    }
  }
]
```

---

### 4.3 数据预览

预览数据资产的内容。

**URL**: `GET /data-mgmt/preview`

**查询参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| path | string | 是 | 资产路径/名称 |
| id | integer | 否 | 资产ID（优先使用） |
| limit | integer | 否 | 20 | 返回行数 |
| offset | integer | 否 | 0 | 偏移量 |

**响应 (200 OK)**:

*数据库表*:
```json
{
  "columns": ["id", "name", "value", "created_at"],
  "data": [
    {"id": 1, "name": "item1", "value": 100, "created_at": "2024-01-01", "_rowid": 1},
    {"id": 2, "name": "item2", "value": 200, "created_at": "2024-01-02", "_rowid": 2}
  ],
  "total": 1000,
  "meta": {
    "source": "clickhouse",
    "editable": true,
    "rowid_col": "id"
  }
}
```

*MinIO桶*:
```json
{
  "columns": ["Key", "Size", "LastModified"],
  "data": [
    {"Key": "file1.csv", "Size": 1024, "LastModified": "2024-01-01T00:00:00", "_rowid": "etag1"}
  ],
  "total": 10,
  "meta": {
    "source": "minio",
    "editable": false
  }
}
```

**meta.editable 说明**:

| 值 | 说明 |
|----|------|
| true | 支持行级编辑（更新/删除） |
| false | 只读（MinIO对象存储） |

---

### 4.4 获取数据结构

获取数据资产的表结构（列名和类型）。

**URL**: `GET /data-mgmt/structure`

**查询参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| path | string | 是 | 资产路径/名称 |
| id | integer | 否 | 资产ID |

**响应 (200 OK)**:

```json
[
  {"name": "id", "type": "Int32", "nullable": false},
  {"name": "name", "type": "String", "nullable": true},
  {"name": "value", "type": "Float64", "nullable": true}
]
```

---

### 4.5 删除数据资产

删除指定的数据资产。

**URL**: `DELETE /data-mgmt/{name_or_id}`

**路径参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name_or_id | string | 是 | 资产名称或ID |

**查询参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| type | string | 否 | 资产类型（可选） |

**响应 (200 OK)**:

```json
{
  "ok": true,
  "message": "Asset xxx deleted"
}
```

**错误响应 (404)**:
```json
{
  "detail": "Asset not found"
}
```

---

### 4.6 删除表行

删除数据表中的指定行。

**URL**: `DELETE /data-mgmt/table/{table_name}/row/{row_id}`

**路径参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| table_name | string | 是 | 表名 |
| row_id | string | 是 | 行ID（主键值） |

**响应 (200 OK)**:

```json
{
  "ok": true
}
```

**错误响应**:

*行不存在 (404)*:
```json
{
  "detail": "Row not found"
}
```

*表无主键 (400)*:
```json
{
  "detail": "Table has no primary key or id column; cannot delete rows"
}
```

---

### 4.7 更新表行

更新数据表中的指定行。

**URL**: `PUT /data-mgmt/table/{table_name}/row/{row_id}`

**路径参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| table_name | string | 是 | 表名 |
| row_id | string | 是 | 行ID（主键值） |

**请求体 (Request Body)**:

```json
{
  "row_id": "1",
  "data": {
    "name": "new_name",
    "value": 999
  }
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| row_id | string | 是 | 行ID（需与URL中的row_id一致） |
| data | object | 是 | 要更新的字段和值 |

**响应 (200 OK)**:

```json
{
  "ok": true
}
```

**错误响应 (400)**:
```json
{
  "detail": "row_id mismatch"
}
```

---

### 4.8 下载数据资产

导出/下载数据资产。

**URL**: `GET /data-mgmt/download/{name_or_id}`

**路径参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name_or_id | string | 是 | 资产名称或ID |

**查询参数**:

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| format | string | 否 | csv | 导出格式：`csv`, `json`, `excel` |

**响应**:

*文件下载 (200 OK)*:
```
Content-Type: text/csv
Content-Disposition: attachment; filename=xxx.csv
[文件二进制内容]
```

*MinIO预签名链接 (200 OK)*:
```json
{
  "status": "minio_links",
  "links": [
    {"key": "file1.csv", "url": "https://minio:9000/..."},
    {"key": "file2.csv", "url": "https://minio:9000/..."}
  ]
}
```

**预签名链接有效期**: 5分钟

---

## 错误码说明

| HTTP状态码 | 说明 |
|------------|------|
| 200 | 请求成功 |
| 201 | 资源创建成功 |
| 400 | 请求参数错误 |
| 404 | 资源不存在 |
| 500 | 服务器内部错误 |

**通用错误响应格式**:

```json
{
  "detail": "错误详细信息"
}
```

---

## 数据类型枚举

| 分类 | 值 | 说明 |
|------|-----|------|
| 数据源类型 | `mysql` | MySQL数据库 |
| 数据源类型 | `clickhouse` | ClickHouse数据库 |
| 数据源类型 | `minio` | MinIO对象存储 |
| 数据源类型 | `csv` | CSV文件 |
| 数据类型 | `TIMESERIES` | 时序数据 |
| 数据类型 | `IMAGE` | 图像数据 |
| 数据类型 | `NER` | 命名实体识别数据 |
| 任务类型 | `full_sync` | 全量同步 |
| 任务类型 | `preprocess` | 数据预处理 |
| 任务类型 | `sync` | 数据同步 |
| 任务状态 | `pending` | 等待执行 |
| 任务状态 | `running` | 执行中 |
| 任务状态 | `success` | 执行成功 |
| 任务状态 | `failed` | 执行失败 |
| 资产类型 | `file` | 本地文件 |
| 资产类型 | `table` | 数据库表 |
| 资产类型 | `bucket` | 对象存储桶 |
