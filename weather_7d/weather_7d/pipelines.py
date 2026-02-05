# weather_7d/pipelines.py
import json
import csv
import os
import re
import pymysql
from datetime import datetime
from weather_7d import settings


class MySQLPipeline:
    """MySQL + CSV 双存储，CSV 每次覆盖，MySQL 智能更新"""

    def __init__(self):
        self.data_dir = 'data'
        os.makedirs(self.data_dir, exist_ok=True)

        self.conn = None
        self.cursor = None
        self.updated_cities = {}
        self.csv_file_handles = {}  # 文件句柄

    def open_spider(self, spider):
        # 连接 MySQL
        self.conn = pymysql.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            charset='utf8mb4',
            autocommit=False
        )
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {settings.MYSQL_DATABASE} CHARACTER SET utf8mb4")
        self.conn.select_db(settings.MYSQL_DATABASE)

        spider.logger.info("✅ MySQL 连接成功")

    def close_spider(self, spider):
        # 提交 MySQL
        if self.conn:
            self.conn.commit()
            for city, count in self.updated_cities.items():
                spider.logger.info(f"📊 {city}: {count} 条")
            self.cursor.close()
            self.conn.close()

        # 关闭所有 CSV 文件
        for city, f in self.csv_file_handles.items():
            f.close()
            spider.logger.info(f"📝 CSV 已保存：weather_{city}_latest.csv")

        spider.logger.info("✅ 全部完成")

    def process_item(self, item, spider):
        city = item.get('city', '未知')

        # 清洗数据
        clean = {k: (item.get(k) or '').strip() for k in item.fields}

        # 初始化 CSV（每个城市只初始化一次）
        if city not in self.csv_file_handles:
            self._init_city_csv(city)

        # 保存
        self._save_mysql(clean, city, spider)
        self._save_csv(clean, city)

        self.updated_cities[city] = self.updated_cities.get(city, 0) + 1
        return item

    def _init_city_csv(self, city):
        """创建 CSV 文件，覆盖模式"""
        filename = os.path.join(self.data_dir, f"weather_{city}_latest.csv")

        # 关键：'w' 模式 = 覆盖，'a' 模式 = 追加
        f = open(filename, 'w', newline='', encoding='utf-8-sig')
        self.csv_file_handles[city] = f

        writer = csv.writer(f)
        writer.writerow(['日期', '星期', '天气', '最高温', '最低温', '风向', '风力', '更新时间'])
        # 保存 writer 到实例，供后续使用
        setattr(self, f'_writer_{city}', writer)

    def _save_mysql(self, item, city, spider):
        """保存到 MySQL"""
        table = f"weather_{re.sub(r'[^\w\u4e00-\u9fa5]', '', city)}"

        # 建表
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `date` VARCHAR(20) NOT NULL,
                `week` VARCHAR(20),
                `weather_condition` VARCHAR(50),
                `high_temp` VARCHAR(20),
                `low_temp` VARCHAR(20),
                `wind_direction` VARCHAR(50),
                `wind_level` VARCHAR(20),
                `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_date` (`date`)
            ) ENGINE=InnoDB CHARSET=utf8mb4
        """)

        # Upsert
        sql = f"""
            INSERT INTO `{table}` (`date`, `week`, `weather_condition`, `high_temp`, `low_temp`, `wind_direction`, `wind_level`, `update_time`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                week=VALUES(week), weather_condition=VALUES(weather_condition),
                high_temp=VALUES(high_temp), low_temp=VALUES(low_temp),
                wind_direction=VALUES(wind_direction), wind_level=VALUES(wind_level),
                update_time=VALUES(update_time)
        """
        try:
            self.cursor.execute(sql, (
                item['date'], item['week'], item['weather_condition'],
                item['high_temp'], item['low_temp'],
                item['wind_direction'], item['wind_level'],
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
            item['date'], item['week'], item['weather_condition'],
            item['high_temp'], item['low_temp'],
            item['wind_direction'], item['wind_level'],
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])