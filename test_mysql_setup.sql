-- 创建测试数据库
CREATE DATABASE IF NOT EXISTS test_import_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 使用测试数据库
USE test_import_db;

-- 创建用户表
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    age INT,
    department VARCHAR(50),
    salary DECIMAL(10,2),
    hire_date DATE,
    status ENUM('active', 'inactive', 'pending') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 创建产品表
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INT DEFAULT 0,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建订单表
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    product_id INT,
    quantity INT NOT NULL,
    total_price DECIMAL(10,2) NOT NULL,
    order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    status ENUM('pending', 'processing', 'completed', 'cancelled') DEFAULT 'pending',
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- 插入测试数据到用户表
INSERT INTO users (name, email, age, department, salary, hire_date, status) VALUES
('张三', 'zhangsan@example.com', 28, '技术部', 15000.00, '2023-01-15', 'active'),
('李四', 'lisi@example.com', 32, '市场部', 18000.00, '2022-06-20', 'active'),
('王五', 'wangwu@example.com', 25, '销售部', 12000.00, '2023-03-10', 'active'),
('赵六', 'zhaoliu@example.com', 35, '技术部', 20000.00, '2021-11-05', 'active'),
('孙七', 'sunqi@example.com', 29, '人力资源部', 14000.00, '2022-09-12', 'inactive'),
('周八', 'zhouba@example.com', 31, '财务部', 16000.00, '2022-04-18', 'active'),
('吴九', 'wujiu@example.com', 27, '市场部', 13500.00, '2023-07-22', 'pending'),
('郑十', 'zhengshi@example.com', 33, '技术部', 19000.00, '2021-08-30', 'active');

-- 插入测试数据到产品表
INSERT INTO products (product_name, category, price, stock_quantity, description) VALUES
('笔记本电脑', '电子产品', 5999.00, 50, '高性能办公笔记本'),
('无线鼠标', '电子产品', 99.00, 200, '人体工学设计'),
('机械键盘', '电子产品', 399.00, 80, '青轴机械键盘'),
('显示器', '电子产品', 1299.00, 30, '27寸4K显示器'),
('办公椅', '办公用品', 899.00, 40, '人体工学办公椅'),
('台灯', '办公用品', 199.00, 100, 'LED护眼台灯'),
('文件夹', '文具用品', 15.00, 500, 'A4文件夹'),
('签字笔', '文具用品', 8.00, 800, '黑色签字笔'),
('笔记本', '文具用品', 25.00, 300, 'A5笔记本'),
('订书机', '办公用品', 45.00, 150, '重型订书机');

-- 插入测试数据到订单表
INSERT INTO orders (user_id, product_id, quantity, total_price, status) VALUES
(1, 1, 1, 5999.00, 'completed'),
(1, 2, 2, 198.00, 'completed'),
(2, 3, 1, 399.00, 'processing'),
(2, 4, 1, 1299.00, 'completed'),
(3, 5, 2, 1798.00, 'completed'),
(3, 6, 3, 597.00, 'pending'),
(4, 7, 10, 150.00, 'completed'),
(4, 8, 5, 40.00, 'completed'),
(5, 9, 20, 500.00, 'cancelled'),
(6, 10, 1, 45.00, 'completed');

-- 验证数据插入
SELECT 'Users表记录数：' AS info, COUNT(*) AS count FROM users
UNION ALL
SELECT 'Products表记录数：', COUNT(*) FROM products
UNION ALL
SELECT 'Orders表记录数：', COUNT(*) FROM orders;
