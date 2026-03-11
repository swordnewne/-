import csv
import os
import re
import pymysql
from datetime import datetime


class MySQLPipeline:
    """MySQL + CSV 双存储，多表模式（每个城市一张表）"""

    def __init__(self):
        self.data_dir = 'data'
        os.makedirs(self.data_dir, exist_ok=True)

        self.conn = None
        self.cursor = None
        self.updated_cities = {}
        self.csv_file_handles = {}

    def open_spider(self, spider):
        # 通过 spider.settings 获取配置
        mysql_host = spider.settings.get('MYSQL_HOST', 'localhost')
        mysql_port = spider.settings.get('MYSQL_PORT', 3306)
        mysql_user = spider.settings.get('MYSQL_USER', 'root')
        mysql_password = spider.settings.get('MYSQL_PASSWORD', '')
        mysql_database = spider.settings.get('MYSQL_DATABASE', 'weather_db')

        # 连接 MySQL
        self.conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            charset='utf8mb4',
            autocommit=False
        )
        self.cursor = self.conn.cursor()

        # 创建数据库（明确指定字符集，避免乱码）
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_database} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        self.conn.select_db(mysql_database)

        # 关键：统一当前会话字符集，解决 UNION 查询冲突
        self.cursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci")

        spider.logger.info("✅ MySQL 多表模式连接成功")

    def close_spider(self, spider):
        if self.conn:
            self.conn.commit()
            for city, count in self.updated_cities.items():
                spider.logger.info(f"📊 {city}: {count} 条")
            self.cursor.close()
            self.conn.close()

        for city, f in self.csv_file_handles.items():
            f.close()
            spider.logger.info(f"📝 CSV 已保存：weather_{city}_latest.csv")

        spider.logger.info("✅ 全部完成")

    def process_item(self, item, spider):
        city = item.get('city', '未知')
        clean = {k: (item.get(k) or '').strip() for k in item.fields}

        if city not in self.csv_file_handles:
            self._init_city_csv(city)

        self._save_mysql(clean, city, spider)
        self._save_csv(clean, city)

        self.updated_cities[city] = self.updated_cities.get(city, 0) + 1
        return item

    def _init_city_csv(self, city):
        """创建 CSV 文件，覆盖模式"""
        filename = os.path.join(self.data_dir, f"weather_{city}_latest.csv")
        f = open(filename, 'w', newline='', encoding='utf-8-sig')
        self.csv_file_handles[city] = f

        writer = csv.writer(f)
        writer.writerow(['日期', '星期', '天气', '最高温', '最低温', '风向', '风力', '更新时间'])
        setattr(self, f'_writer_{city}', writer)

    def _save_mysql(self, item, city, spider):
        """保存到 MySQL（每个城市独立表）"""
        # 清理表名（移除特殊字符）
        table = f"weather_{re.sub(r'[^\w\u4e00-\u9fa5]', '', city)}"

        try:
            # 建表 - 使用完整日期格式，字符集统一
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS `{table}` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `date` DATE NOT NULL COMMENT '完整日期(YYYY-MM-DD)',
                    `week` VARCHAR(20) COMMENT '星期',
                    `weather_condition` VARCHAR(50) COMMENT '天气状况',
                    `high_temp` VARCHAR(20) COMMENT '最高温',
                    `low_temp` VARCHAR(20) COMMENT '最低温',
                    `wind_direction` VARCHAR(50) COMMENT '风向',
                    `wind_level` VARCHAR(20) COMMENT '风力',
                    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY `uk_date` (`date`)
                ) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """)

            # Upsert（插入或更新）
            sql = f"""
                INSERT INTO `{table}` 
                (`date`, `week`, `weather_condition`, `high_temp`, `low_temp`, `wind_direction`, `wind_level`, `update_time`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    week=VALUES(week), 
                    weather_condition=VALUES(weather_condition),
                    high_temp=VALUES(high_temp), 
                    low_temp=VALUES(low_temp),
                    wind_direction=VALUES(wind_direction), 
                    wind_level=VALUES(wind_level),
                    update_time=VALUES(update_time)
            """
            self.cursor.execute(sql, (
                item['date'],  # 现在格式是 2026-03-10
                item['week'],
                item['weather_condition'],
                item['high_temp'],
                item['low_temp'],
                item['wind_direction'],
                item['wind_level'],
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            spider.logger.error(f"❌ {city} MySQL错误: {e}")

    def _save_csv(self, item, city):
        """保存到 CSV"""
        writer = getattr(self, f'_writer_{city}')
        writer.writerow([
            item['date'],  # CSV 也显示完整日期
            item['week'],
            item['weather_condition'],
            item['high_temp'],
            item['low_temp'],
            item['wind_direction'],
            item['wind_level'],
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])