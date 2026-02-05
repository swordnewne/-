# weather_7d/spiders/weather_spider.py
import scrapy
import re
from weather_7d.items import WeatherItem


class WeatherSpider(scrapy.Spider):
    name = 'weather'
    allowed_domains = ['weather.com.cn']

    city_codes = {
        '北京': '101010100', '上海': '101020100', '广州': '101280101',
        '深圳': '101280601', '杭州': '101210101', '成都': '101270101',
        '福州': '101230101', '厦门': '101230201', '泉州': '101230501',
        '南京': '101190101', '武汉': '101200101', '西安': '101110101',
        '重庆': '101040100', '天津': '101030100', '苏州': '101190401',
        '长沙': '101250101',
    }

    def __init__(self, city=None, cities=None, **kwargs):
        super().__init__(**kwargs)
        self.start_urls = []
        target_cities = []

        if cities:
            target_cities = [c.strip() for c in cities.split(',') if c.strip() in self.city_codes]
        elif city and city in self.city_codes:
            target_cities = [city]
        else:
            target_cities = ['北京']

        for c in target_cities:
            code = self.city_codes[c]
            self.start_urls.append(f'http://www.weather.com.cn/weather/{code}.shtml')

    def parse(self, response):
        """解析天气数据 - 修复风向风力提取"""
        # 提取城市名
        city = response.xpath('//div[@class="crumbs fl"]//a[last()]/text()').get('')
        if not city:
            city = response.xpath('//h1/text()').get('')
        if not city:
            url_code = re.search(r'/weather/(\d+)\.shtml', response.url)
            for name, c in self.city_codes.items():
                if c == url_code.group(1):
                    city = name
                    break

        city = city.replace('天气', '').strip() or '未知城市'
        self.logger.info(f"🌤️ 解析城市：{city}")

        # 提取 7 日天气 - 新版中国天气网结构
        # 尝试多种 XPath 路径
        weather_list = response.xpath('//ul[@class="t clearfix"]/li')

        if not weather_list:
            self.logger.error(f"❌ {city}: 未找到天气数据")
            return

        for index, day in enumerate(weather_list[:7], 1):
            item = WeatherItem()
            item['city'] = city

            # ========== 日期 ==========
            date_html = day.xpath('.//h1/text()').get('')
            m = re.search(r'(\d{1,2}日)[（(](.+?)[）)]', date_html)
            item['date'] = m.group(1) if m else date_html
            item['week'] = m.group(2) if m else ''

            # ========== 天气状况 ==========
            # 尝试多种方式提取
            weather = day.xpath('.//p[@class="wea"]/text()').get('')
            if not weather:
                weather = day.xpath('.//p[@class="wea"]/@title').get('')
            item['weather_condition'] = weather or ''

            # ========== 温度 ==========
            high = day.xpath('.//p[@class="tem"]/span/text()').get('')
            low = day.xpath('.//p[@class="tem"]/i/text()').get('')
            item['high_temp'] = high + '℃' if high else ''
            item['low_temp'] = low.replace('℃', '') + '℃' if low else ''

            # ========== 风向风力（关键修复）==========
            # 中国天气网新版结构：p[@class="win"] 下有 em 和 i 标签

            # 风向：em 标签下的 span 或 i 标签的 title 属性
            wind_directions = day.xpath('.//p[@class="win"]/em//span/@title').getall()
            if not wind_directions:
                # 备用：直接取 em 下的文本
                wind_directions = day.xpath('.//p[@class="win"]/em//text()').getall()

            # 清理并去重
            wind_directions = [w.strip() for w in wind_directions if w.strip()]
            item['wind_direction'] = '/'.join(wind_directions) if wind_directions else ''

            # 风力：i 标签的文本
            wind_level = day.xpath('.//p[@class="win"]/i/text()').get('')
            if not wind_level:
                # 备用：从 title 属性提取
                wind_level = day.xpath('.//p[@class="win"]/i/@title').get('')
            item['wind_level'] = wind_level.strip() if wind_level else ''

            # 如果还是空的，用正则从 HTML 中提取
            if not item['wind_direction'] or not item['wind_level']:
                win_html = day.xpath('.//p[@class="win"]').get('')
                if win_html:
                    # 提取风向
                    if not item['wind_direction']:
                        winds = re.findall(r'title="(.{1,3}风)"', win_html)
                        item['wind_direction'] = '/'.join(winds)
                    # 提取风力
                    if not item['wind_level']:
                        level = re.search(r'<i>(.*?)</i>', win_html)
                        if level:
                            item['wind_level'] = re.sub(r'<[^>]+>', '', level.group(1))

            # 调试日志
            self.logger.info(
                f"  第{index}天: {item['date']}{item['week']} | "
                f"{item['weather_condition']} | "
                f"{item['high_temp']}/{item['low_temp']} | "
                f"风向:{item['wind_direction'] or '空'} | "
                f"风力:{item['wind_level'] or '空'}"
            )

            yield item