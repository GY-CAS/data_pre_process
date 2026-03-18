import pymysql
from pymysql.err import OperationalError, ProgrammingError

# -------------------------- 数据库配置（适配你的环境） --------------------------
DB_CONFIG = {
    "host": "192.168.1.19",  # 宿主机IP（或填 localhost/127.0.0.1）
    "port": 3106,            # 宿主机映射端口（不是容器3306）
    "user": "root",          # 用户名（已改为 root）
    "password": "12345678",  # 密码
    "database": "test_db",   # 要创建并连接的数据库
    "charset": "utf8mb4"     # 字符集（兼容中文/emoji）
}

# -------------------------- 核心连接与操作函数 --------------------------
def connect_mysql():
    """
    建立 MySQL 连接（先连接 MySQL 服务，再创建数据库）
    返回：数据库连接对象 / None（连接失败）
    """
    try:
        # 第一步：先连接 MySQL 服务（不指定具体数据库）
        conn = pymysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            charset=DB_CONFIG["charset"]
        )
        print("✅ 成功连接 MySQL 服务！")
        
        # 第二步：创建数据库（如果不存在）
        cursor = conn.cursor()
        create_db_sql = f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} DEFAULT CHARACTER SET {DB_CONFIG['charset']} COLLATE {DB_CONFIG['charset']}_unicode_ci;"
        cursor.execute(create_db_sql)
        print(f"✅ 数据库 {DB_CONFIG['database']} 创建成功（或已存在）！")
        cursor.close()
        
        # 第三步：切换到目标数据库
        conn.select_db(DB_CONFIG["database"])
        print(f"✅ 已切换到 {DB_CONFIG['database']} 数据库！")
        return conn
    
    except OperationalError as e:
        print(f"❌ 数据库连接失败：{e}")
        # 常见错误排查提示
        if "Access denied" in str(e):
            print("  - 检查用户名/密码是否正确")
        elif "Connection refused" in str(e):
            print("  - 检查主机 IP/端口是否正确，或 MySQL 容器是否运行")
        return None
    except Exception as e:
        print(f"❌ 未知错误：{e}")
        return None

def create_table(conn):
    """
    创建测试表（用户行为表）
    """
    try:
        cursor = conn.cursor()
        create_sql = """
        CREATE TABLE IF NOT EXISTS user_behavior (
            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键 ID',
            user_id VARCHAR(32) NOT NULL COMMENT '用户 ID',
            behavior_type ENUM('click','view','purchase','collect') NOT NULL COMMENT '行为类型',
            product_id VARCHAR(32) NOT NULL COMMENT '商品 ID',
            behavior_time DATETIME NOT NULL COMMENT '行为时间'
        ) COMMENT '用户行为记录表';
        """
        cursor.execute(create_sql)
        conn.commit()
        print("✅ 表 user_behavior 创建成功（或已存在）！")
        cursor.close()
    except ProgrammingError as e:
        print(f"❌ 建表失败：{e}")
        conn.rollback()
    except Exception as e:
        print(f"❌ 未知错误：{e}")
        conn.rollback()

def create_new_table(conn):
    """
    创建新表（用户信息表）
    """
    try:
        cursor = conn.cursor()
        create_sql = """
        CREATE TABLE IF NOT EXISTS user_info (
            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键 ID',
            user_id VARCHAR(32) NOT NULL UNIQUE COMMENT '用户 ID',
            username VARCHAR(64) NOT NULL COMMENT '用户名',
            email VARCHAR(128) COMMENT '邮箱',
            phone VARCHAR(20) COMMENT '手机号',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
        ) COMMENT '用户信息表';
        """
        cursor.execute(create_sql)
        conn.commit()
        print("✅ 表 user_info 创建成功（或已存在）！")
        cursor.close()
    except ProgrammingError as e:
        print(f"❌ 建表失败：{e}")
        conn.rollback()
    except Exception as e:
        print(f"❌ 未知错误：{e}")
        conn.rollback()

def insert_data(conn, user_id, behavior_type, product_id, behavior_time):
    """
    插入单条数据
    """
    try:
        cursor = conn.cursor()
        insert_sql = """
        INSERT INTO user_behavior (user_id, behavior_type, product_id, behavior_time)
        VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_sql, (user_id, behavior_type, product_id, behavior_time))
        conn.commit()
        print(f"✅ 数据插入成功，ID: {cursor.lastrowid}")
        cursor.close()
    except Exception as e:
        print(f"❌ 插入数据失败：{e}")
        conn.rollback()

def insert_user_info(conn, user_id, username, email=None, phone=None):
    """
    插入用户信息到新表
    """
    try:
        cursor = conn.cursor()
        insert_sql = """
        INSERT INTO user_info (user_id, username, email, phone)
        VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_sql, (user_id, username, email, phone))
        conn.commit()
        print(f"✅ 用户信息插入成功，ID: {cursor.lastrowid}")
        cursor.close()
    except Exception as e:
        print(f"❌ 插入用户信息失败：{e}")
        conn.rollback()

def query_data(conn):
    """
    查询数据示例
    """
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        query_sql = "SELECT * FROM user_behavior LIMIT 10;"
        cursor.execute(query_sql)
        results = cursor.fetchall()
        print("\n📋 查询 user_behavior 前 10 条数据：")
        for row in results:
            print(row)
        cursor.close()
        return results
    except Exception as e:
        print(f"❌ 查询数据失败：{e}")
        return None

def query_user_info(conn):
    """
    查询新用户表数据
    """
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        query_sql = "SELECT * FROM user_info LIMIT 10;"
        cursor.execute(query_sql)
        results = cursor.fetchall()
        print("\n📋 查询 user_info 前 10 条数据：")
        for row in results:
            print(row)
        cursor.close()
        return results
    except Exception as e:
        print(f"❌ 查询用户信息失败：{e}")
        return None

# -------------------------- 主函数（执行流程） --------------------------
if __name__ == "__main__":
    # 1. 建立连接（自动创建数据库）
    db_conn = connect_mysql()
    if not db_conn:
        exit(1)
    
    # 2. 创建原有表
    create_table(db_conn)
    
    # 3. 创建新表（user_info）
    create_new_table(db_conn)
    
    # 4. 插入测试数据
    insert_data(
        db_conn,
        user_id="U001",
        behavior_type="click",
        product_id="P001",
        behavior_time="2026-03-19 10:00:00"
    )
    
    # 5. 插入用户信息到新表
    insert_user_info(
        db_conn,
        user_id="U001",
        username="测试用户",
        email="test@example.com",
        phone="13800138000"
    )
    
    # 6. 查询数据
    query_data(db_conn)
    query_user_info(db_conn)
    
    # 7. 关闭连接
    db_conn.close()
    print("\n🔌 数据库连接已关闭")