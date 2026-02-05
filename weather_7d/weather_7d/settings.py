# weather_7d/settings.py

BOT_NAME = 'weather_7d'

SPIDER_MODULES = ['weather_7d.spiders']
NEWSPIDER_MODULE = 'weather_7d.spiders'

ROBOTSTXT_OBEY = False
DOWNLOAD_DELAY = 1
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