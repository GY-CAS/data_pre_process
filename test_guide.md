# 数据源测试指南

## 测试环境概览

### 外部数据源测试数据

| 数据源 | 图片数据 (IMAGE) | 时序数据 (TIMESERIES) | 文本数据 (NER) |
|--------|------------------|----------------------|----------------|
| **MySQL** | `image_data` 表 (5条) | `timeseries_data` 表 (8条) | `ner_data` 表 (7条) |
| **ClickHouse** | `ck_image` 表 (0条*) | `ck_timeseries` 表 (16条) | `ck_ner` 表 (0条*) |
| **MinIO** | `image-data` 存储桶 | `timeseries-data` 存储桶 | `ner-data` 存储桶 |

> *注意：ClickHouse的中文数据插入有问题，但时序数据已足够测试

---

## 第一步：添加数据源

请在浏览器中打开 http://localhost:13000

### 1.1 添加MySQL数据源

1. 点击 "添加数据源"
2. 填写以下信息：
   - 名称：MySQL测试源
   - 类型：MySQL
   - 数据类型：IMAGE（图片）
   - 连接信息：
     - 主机：localhost
     - 端口：3306
     - 用户：root
     - 密码：''
     - 数据库：test_db
3. 点击 "测试连接"
4. 点击 "创建"

### 1.2 添加ClickHouse数据源

1. 点击 "添加数据源"
2. 填写以下信息：
   - 名称：ClickHouse测试源
   - 类型：ClickHouse
   - 数据类型：TIMESERIES（时序）
   - 连接信息：
     - 主机：localhost
     - 端口：9002
     - 用户：default
     - 密码：default
     - 数据库：default
3. 点击 "测试连接"
4. 点击 "创建"

### 1.3 添加MinIO数据源

1. 点击 "添加数据源"
2. 填写以下信息：
   - 名称：MinIO测试源
   - 类型：MinIO (S3)
   - 数据类型：NER（文本）
   - 连接信息：
     - Endpoint：http://localhost:9000
     - Access Key：minioadmin
     - Secret Key：minioadmin
3. 点击 "测试连接"
4. 点击 "创建"

---

## 第二步：创建同步任务

在 "任务" 页面创建以下同步任务：

### 2.1 MySQL → MinIO（图片数据）

- 任务名称：MySQL图片同步到MinIO
- 源数据库：选择 MySQL测试源
- 源表：image_data
- 目标存储：system_minio
- 目标表：mysql_image_to_minio
- 同步模式：overwrite

### 2.2 MySQL → MinIO（时序数据）

- 任务名称：MySQL时序同步到MinIO
- 源数据库：选择 MySQL测试源
- 源表：timeseries_data
- 目标存储：system_minio
- 目标表：mysql_timeseries_to_minio
- 同步模式：overwrite

### 2.3 MySQL → MinIO（文本数据）

- 任务名称：MySQL文本同步到MinIO
- 源数据库：选择 MySQL测试源
- 源表：ner_data
- 目标存储：system_minio
- 目标表：mysql_ner_to_minio
- 同步模式：overwrite

### 2.4 ClickHouse → MinIO（时序数据）

- 任务名称：ClickHouse时序同步到MinIO
- 源数据库：选择 ClickHouse测试源
- 源表：ck_timeseries
- 目标存储：system_minio
- 目标表：ck_timeseries_to_minio
- 同步模式：overwrite

### 2.5 MySQL → ClickHouse（图片数据）

- 任务名称：MySQL图片同步到ClickHouse
- 源数据库：选择 MySQL测试源
- 源表：image_data
- 目标存储：system_clickhouse
- 目标表：mysql_image_to_ck
- 同步模式：overwrite

### 2.6 MySQL → ClickHouse（时序数据）

- 任务名称：MySQL时序同步到ClickHouse
- 源数据库：选择 MySQL测试源
- 源表：timeseries_data
- 目标存储：system_clickhouse
- 目标表：mysql_timeseries_to_ck
- 同步模式：overwrite

---

## 第三步：运行同步任务

1. 点击每个任务的 "运行" 按钮
2. 等待任务完成
3. 检查任务状态

---

## 第四步：验证数据管理和预览

1. 打开 "数据管理" 页面
2. 查看同步后的数据资产
3. 点击 "预览" 按钮查看数据内容

---

## 测试矩阵

| 源数据源 | 目标存储 | 数据类型 | 预期结果 |
|----------|----------|----------|----------|
| MySQL | MinIO | IMAGE | ✅ 应成功 |
| MySQL | MinIO | TIMESERIES | ✅ 应成功 |
| MySQL | MinIO | NER | ✅ 应成功 |
| ClickHouse | MinIO | TIMESERIES | ✅ 应成功 |
| MySQL | ClickHouse | IMAGE | ✅ 应成功 |
| MySQL | ClickHouse | TIMESERIES | ✅ 应成功 |
| MinIO | MySQL | IMAGE | 待测试 |
| MinIO | ClickHouse | NER | 待测试 |
