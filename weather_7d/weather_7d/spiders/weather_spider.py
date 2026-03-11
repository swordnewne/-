import scrapy
import re
import datetime
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
        """解析天气数据 - 修复重复符号问题"""
        now = datetime.datetime.now()
        current_day = now.day
        current_month = now.month
        current_year = now.year
        current_weekday = now.weekday()
        weekdays = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']

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

        # 提取 7 日天气
        weather_list = response.xpath('//ul[@class="t clearfix"]/li')

        if not weather_list:
            self.logger.error(f"❌ {city}: 未找到天气数据")
            return

        for index, day in enumerate(weather_list[:7], 0):
            item = WeatherItem()
            item['city'] = city

            # ========== 日期处理 ==========
            date_html = day.xpath('.//h1/text()').get('')
            m = re.search(r'(\d{1,2})日[（(](.+?)[）)]', date_html)

            if m:
                day_num = int(m.group(1))
                week_raw = m.group(2)

                # 计算正确的年月
                if day_num < current_day:
                    if current_month == 12:
                        target_year = current_year + 1
                        target_month = 1
                    else:
                        target_year = current_year
                        target_month = current_month + 1
                else:
                    target_year = current_year
                    target_month = current_month

                item['date'] = f"{target_year}-{target_month:02d}-{day_num:02d}"

                # 星期处理
                if week_raw in ['今天', '明天', '后天']:
                    offset = {'今天': 0, '明天': 1, '后天': 2}[week_raw]
                    target_weekday = (current_weekday + offset) % 7
                    item['week'] = weekdays[target_weekday]
                else:
                    item['week'] = week_raw
            else:
                fallback_date = now + datetime.timedelta(days=index)
                item['date'] = fallback_date.strftime('%Y-%m-%d')
                item['week'] = weekdays[(current_weekday + index) % 7]

            # ========== 天气状况 ==========
            weather = day.xpath('.//p[@class="wea"]/text()').get('')
            if not weather:
                weather = day.xpath('.//p[@class="wea"]/@title').get('')
            item['weather_condition'] = weather or ''

            # ========== 温度（修复重复℃问题）==========
            high = day.xpath('.//p[@class="tem"]/span/text()').get('')
            low = day.xpath('.//p[@class="tem"]/i/text()').get('')

            # 如果已经包含℃就不再添加
            item['high_temp'] = high if high and '℃' in high else (high + '℃' if high else '')
            item['low_temp'] = low if low and '℃' in low else (low + '℃' if low else '')

            # ========== 风向风力（修复重复问题）==========
            wind_directions = day.xpath('.//p[@class="win"]/em//span/@title').getall()
            if not wind_directions:
                wind_directions = day.xpath('.//p[@class="win"]/em//text()').getall()

            # 清理并去重（关键修复）
            wind_directions = [w.strip() for w in wind_directions if w.strip()]
            # 使用 dict.fromkeys() 去重并保持顺序（Python 3.7+）
            wind_directions = list(dict.fromkeys(wind_directions))
            item['wind_direction'] = '/'.join(wind_directions) if wind_directions else ''

            wind_level = day.xpath('.//p[@class="win"]/i/text()').get('')
            if not wind_level:
                wind_level = day.xpath('.//p[@class="win"]/i/@title').get('')
            item['wind_level'] = wind_level.strip() if wind_level else ''

            # 调试日志
            self.logger.info(
                f"  第{index + 1}天: {item['date']} {item['week']} | "
                f"{item['weather_condition']} | "
                f"{item['high_temp']}/{item['low_temp']} | "
                f"风向:{item['wind_direction'] or '空'} | "
                f"风力:{item['wind_level'] or '空'}"
            )

            yield item