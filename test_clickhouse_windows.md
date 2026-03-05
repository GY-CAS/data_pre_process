# Windows 环境下测试 ClickHouse 数据指南

## 步骤 1: 在 Windows 上安装 ClickHouse

### 方法 1: 使用 Docker (推荐)

1. **安装 Docker Desktop for Windows**
   - 下载地址: https://www.docker.com/products/docker-desktop
   - 按照安装向导完成安装
   - 启动 Docker Desktop

2. **运行 ClickHouse 容器**
   - 打开 PowerShell 或命令提示符
   - 运行以下命令:
     ```powershell
     docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 --ulimit nofile=262144:262144 yandex/clickhouse-server
     ```

3. **验证 ClickHouse 是否运行**
   - 运行:
     ```powershell
     docker ps
     ```
   - 应该看到 clickhouse-server 容器正在运行

### 方法 2: 直接安装 ClickHouse (Windows 原生)

1. **下载 ClickHouse**
   - 访问 https://github.com/ClickHouse/ClickHouse/releases
   - 下载 Windows 版本的安装包 (通常是 `.exe` 文件)

2. **安装 ClickHouse**
   - 运行安装程序，按照向导完成安装
   - 安装完成后，ClickHouse 服务应该会自动启动

## 步骤 2: 验证 ClickHouse 连接

1. **测试 HTTP 接口**
   - 打开浏览器，访问: http://localhost:8123
   - 应该看到 ClickHouse 的 HTTP 接口页面

2. **测试原生接口**
   - 打开 PowerShell 或命令提示符
   - 运行:
     ```powershell
     telnet localhost 9000
     ```
   - 如果连接成功，说明 ClickHouse 原生接口正常

## 步骤 3: 在系统中添加 ClickHouse 数据源

1. **打开数据源管理页面**
   - 访问前端界面: http://localhost:13000
   - 点击 "数据源" 菜单

2. **添加 ClickHouse 数据源**
   - 点击 "添加数据源" 按钮
   - 填写以下信息:
     - 名称: 测试 ClickHouse
     - 类型: ClickHouse
     - 数据类型: 选择一个合适的类型 (例如 TIMESERIES)
     - 连接信息:
       - 主机: localhost
       - 端口: 9002
       - 用户: default
       - 密码: default
       - 数据库: default

3. **测试连接**
   - 点击 "测试连接" 按钮
   - 如果连接成功，会显示 "连接成功" 的提示

4. **保存数据源**
   - 点击 "创建" 按钮保存数据源

## 步骤 4: 创建 ClickHouse 测试数据

1. **连接到 ClickHouse**
   - 使用 ClickHouse 客户端工具或 HTTP 接口
   - 例如，使用 curl 命令:
     ```powershell
     curl -X POST http://localhost:8123/ -d "CREATE TABLE test_table (id Int32, name String, value Float64, created_at DateTime) ENGINE = MergeTree() ORDER BY id"
     ```

2. **插入测试数据**
   - 运行:
     ```powershell
     curl -X POST http://localhost:8123/ -d "INSERT INTO test_table VALUES (1, 'test1', 100.5, now()), (2, 'test2', 200.7, now()), (3, 'test3', 300.9, now())"
     ```

3. **验证数据**
   - 运行:
     ```powershell
     curl -X POST http://localhost:8123/ -d "SELECT * FROM test_table"
     ```
   - 应该看到插入的测试数据

## 步骤 5: 同步 ClickHouse 数据到系统

1. **创建同步任务**
   - 点击 "任务" 菜单
   - 点击 "创建任务" 按钮
   - 填写以下信息:
     - 任务名称: 同步 ClickHouse 测试数据
     - 源数据库: 选择刚才创建的 ClickHouse 数据源
     - 源表: test_table
     - 目标存储: 选择一个合适的目标 (例如 system_clickhouse 或 system_minio)

2. **运行同步任务**
   - 点击 "运行" 按钮
   - 等待任务完成

3. **验证同步结果**
   - 点击 "数据管理" 菜单
   - 查看同步的数据是否显示在列表中
   - 点击 "预览" 按钮查看数据内容

## 步骤 6: 测试 ClickHouse 数据操作

1. **预览数据**
   - 在数据管理页面，找到同步的 ClickHouse 数据
   - 点击 "预览" 按钮
   - 应该能正常看到数据内容

2. **导出数据**
   - 点击 "下载" 按钮
   - 选择导出格式 (CSV、JSON 等)
   - 验证导出文件是否包含正确的数据

3. **删除数据**
   - 点击 "删除" 按钮
   - 确认删除操作
   - 验证数据是否从列表中消失

## 故障排除

### 连接失败
- 检查 ClickHouse 服务是否正在运行
- 验证端口是否正确 (默认 9000 用于原生接口，8123 用于 HTTP 接口)
- 确认防火墙没有阻止连接

### 同步失败
- 检查源表是否存在
- 验证用户权限是否足够
- 查看任务日志了解详细错误信息

### 数据预览失败
- 检查同步任务是否成功完成
- 验证目标存储是否正确配置
- 查看后端日志了解详细错误信息

## 总结

通过以上步骤，您应该能够在 Windows 环境下成功测试 ClickHouse 数据的接入、同步和操作。如果遇到任何问题，请检查相关配置和日志，或者参考 ClickHouse 官方文档获取更多帮助。
