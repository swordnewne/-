# weather_7d/settings.py

BOT_NAME = 'weather_7d'

SPIDER_MODULES = ['weather_7d.spiders']
NEWSPIDER_MODULE = 'weather_7d.spiders'

ROBOTSTXT_OBEY = False
DOWNLOAD_DELAY = 1.5          # 每个请求间隔 1.5 秒
RANDOMIZE_DOWNLOAD_DELAY = True  # 随机延迟 0.5-1.5 倍
CONCURRENT_REQUESTS = 2       # 并发数降低为 2
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

ITEM_PIPELINES = {
    'weather_7d.pipelines.MySQLPipeline': 300,
}

# MySQL 配置（新增）
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = '367367Aa'
MYSQL_DATABASE = 'weather_db'
