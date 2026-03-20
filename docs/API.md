# API 接口文档

## 概述

数据预处理系统 API，基于 FastAPI 构建，提供数据源管理、任务调度、数据资产管理等功能。

**基础URL**: `http://localhost:8007` (默认)

**API文档**: `http://localhost:8007/docs`

---

## 认证

当前版本未实现认证机制，所有接口可公开访问。

---

## 数据源管理 (DataSources)

### 创建数据源

**端点**: `POST /datasources/`

创建新的数据源配置。

**请求体**:
```json
{
  "name": "string",
  "type": "mysql | clickhouse | minio | rest_api",
  "data_type": "TIMESERIES | IMAGE | NER | TEXT",
  "connection_config": {},
  "description": "string"
}
```

**响应**:
```json
{
  "id": 1,
  "name": "string",
  "type": "mysql",
  "data_type": "TIMESERIES",
  "connection_config": {},
  "description": "string",
  "created_at": "2024-01-01T00:00:00"
}
```

---

### 获取数据源列表

**端点**: `GET /datasources/`

获取所有数据源，支持分页。

**查询参数**:
| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| skip | int | 0 | 跳过记录数 |
| limit | int | 100 | 返回记录数 |

**响应**:
```json
{
  "data": [...],
  "total": 10,
  "skip": 0,
  "limit": 100
}
```

---

### 搜索数据源

**端点**: `GET /datasources/search`

根据条件搜索数据源。

**查询参数**:
| 参数 | 类型 | 说明 |
|------|------|------|
| name | string | 按名称模糊匹配 |
| type | string | 按类型精确匹配 |
| data_type | string | 按数据类型匹配 |
| skip | int | 分页偏移 |
| limit | int | 返回数量 |

---

### 获取单个数据源

**端点**: `GET /datasources/{datasource_id}`

---

### 获取数据源关联任务

**端点**: `GET /datasources/{datasource_id}/related-tasks`

获取使用该数据源的所有任务。

---

### 获取数据源元数据

**端点**: `GET /datasources/{datasource_id}/metadata`

获取数据源的表结构、字段信息等元数据。

---

### 删除数据源

**端点**: `DELETE /datasources/{datasource_id}`

---

### 测试数据源连接

**端点**: `POST /datasources/test-connection`

测试数据源连接是否可用。

**请求体**:
```json
{
  "type": "mysql",
  "connection_config": {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "test_db"
  }
}
```

**响应**:
```json
{
  "success": true,
  "message": "Connection successful"
}
```

---

## 任务管理 (Tasks)

### 创建任务

**端点**: `POST /tasks/`

创建新的数据处理任务。

**请求体**:
```json
{
  "name": "string",
  "task_type": "spark | sync",
  "config": {},
  "description": "string"
}
```

---

### 获取任务列表

**端点**: `GET /tasks/`

获取所有任务，支持分页和名称过滤。

**查询参数**:
| 参数 | 类型 | 说明 |
|------|------|------|
| skip | int | 分页偏移 |
| limit | int | 返回数量 |
| name | string | 按名称过滤 |

**响应**:
```json
{
  "items": [...],
  "total": 10
}
```

---

### 获取单个任务

**端点**: `GET /tasks/{task_id}`

---

### 执行任务

**端点**: `POST /tasks/{task_id}/run`

异步执行任务。

---

### 删除单个任务

**端点**: `DELETE /tasks/{task_id}`

---

### 批量删除任务

**端点**: `DELETE /tasks/`

批量删除任务。

**请求体**:
```json
[1, 2, 3]
```

---

## 数据资产管理 (Data Management)

### 获取资产列表

**端点**: `GET /data-mgmt/assets`

获取所有数据资产（包括本地文件和同步的表）。

**响应**:
```json
{
  "data": [
    {
      "id": 1,
      "name": "data.csv",
      "type": "file",
      "path": "data/data.csv",
      "size": "1.5 MB",
      "source": "Local File",
      "rows": 0,
      "data_type": null
    }
  ],
  "total": 10
}
```

**资产类型**:
- `file`: 本地文件 (CSV, Parquet, JSON)
- `table`: 数据库表
- `bucket`: MinIO 存储桶

---

### 搜索资产

**端点**: `GET /data-mgmt/assets/search`

根据条件搜索资产。

**查询参数**:
| 参数 | 类型 | 说明 |
|------|------|------|
| name | string | 按名称模糊匹配 |
| type | string | 按类型匹配 (file/table/bucket) |
| data_type | string | 数据类型匹配 |

---

### 数据预览

**端点**: `GET /data-mgmt/preview`

预览数据内容，支持分页和排序。

**查询参数**:
| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| path | string | 必填 | 数据文件路径或表名 |
| id | int | null | 资产ID |
| page | int | 1 | 页码 |
| pageSize | int | 20 | 每页条数 |
| sortField | string | null | 排序字段 |
| sortOrder | string | null | 排序方式 (asc/desc) |

**响应**:
```json
{
  "data": [...],
  "pagination": {
    "page": 1,
    "pageSize": 20,
    "total": 100,
    "totalPages": 5
  },
  "sort": {
    "field": "column_name",
    "order": "asc"
  },
  "columns": ["col1", "col2", "col3"]
}
```

---

### 获取数据结构

**端点**: `GET /data-mgmt/structure`

获取数据文件的结构信息（列名、类型）。

**查询参数**:
| 参数 | 类型 | 说明 |
|------|------|------|
| path | string | 文件路径或表名 |
| id | int | 资产ID |

---

### 删除资产

**端点**: `DELETE /data-mgmt/{name_or_id}`

删除指定的数据资产。

---

### 更新表数据

**端点**: `PUT /data-mgmt/table/{table_name}/row/{row_id}`

更新表中指定行的数据。

**请求体**:
```json
{
  "row_id": "1",
  "data": {
    "column1": "value1",
    "column2": "value2"
  }
}
```

---

### 删除表数据

**端点**: `DELETE /data-mgmt/table/{table_name}/row/{row_id}`

删除表中指定行的数据。

---

### 下载数据

**端点**: `GET /data-mgmt/download/{name_or_id}`

下载数据资产为指定格式。

**查询参数**:
| 参数 | 类型 | 默认值 |
|------|------|--------|
| format | string | csv |

**支持格式**: csv, json, parquet

---

### 获取完整资产列表

**端点**: `GET /data-mgmt/assets-complete`

获取包含更多详细信息的资产列表。

---

## 审计日志 (Audit)

### 获取日志列表

**端点**: `GET /audit/`

获取审计日志，支持分页和过滤。

**查询参数**:
| 参数 | 类型 | 说明 |
|------|------|------|
| skip | int | 分页偏移 |
| limit | int | 返回数量 |
| user_id | string | 按用户ID过滤 |
| action | string | 按操作类型过滤 |
| resource | string | 按资源名称过滤 |

**响应**:
```json
{
  "items": [
    {
      "id": 1,
      "user_id": "admin",
      "action": "create_datasource",
      "resource": "my_datasource",
      "details": "Type: mysql",
      "timestamp": "2024-01-01T00:00:00"
    }
  ],
  "total": 100
}
```

---

### 删除日志

**端点**: `DELETE /audit/`

批量删除审计日志。

**请求体**:
```json
[1, 2, 3]
```

---

### 创建日志

**端点**: `POST /audit/`

手动创建审计日志。

**请求体**:
```json
{
  "user_id": "admin",
  "action": "create_task",
  "resource": "task_name",
  "details": "Task details"
}
```

---

## 健康检查

### 健康状态

**端点**: `GET /health`

检查服务健康状态。

**响应**:
```json
{
  "status": "healthy",
  "database": "connected"
}
```

---

## 错误响应

所有接口可能返回以下错误：

| 状态码 | 说明 |
|--------|------|
| 400 | 请求参数错误 |
| 404 | 资源不存在 |
| 422 | 数据验证失败 |
| 500 | 服务器内部错误 |

**错误响应格式**:
```json
{
  "detail": "Error message here"
}
```
