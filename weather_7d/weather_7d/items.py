# weather_7d/items.py
import scrapy

class WeatherItem(scrapy.Item):
    city = scrapy.Field()
    date = scrapy.Field()
    week = scrapy.Field()
    weather_condition = scrapy.Field()
    high_temp = scrapy.Field()
    low_temp = scrapy.Field()
    wind_direction = scrapy.Field()
    wind_level = scrapy.Field()