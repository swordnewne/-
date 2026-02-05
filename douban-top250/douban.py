import requests, csv, time
from bs4 import BeautifulSoup
from tqdm import tqdm
from sqlalchemy import create_engine, Column, SmallInteger, String, DECIMAL, TIMESTAMP
from sqlalchemy.orm import declarative_base, sessionmaker

# 1. 连库
USERNAME, PASSWORD = 'root', '367367Aa'   # 换成你自己的
engine = create_engine(
    f'mysql+pymysql://{USERNAME}:{PASSWORD}@127.0.0.1:3306/douban?charset=utf8mb4',
    future=True, echo=False)
Base = declarative_base()

# 2. 定义表 ORM（和刚才 SQL 对应）
class MovieTop250(Base):
    __tablename__ = 'movie_top250'
    id         = Column(SmallInteger, primary_key=True, autoincrement=True)
    rank250    = Column(SmallInteger, nullable=False, unique=True)  # 唯一
    title      = Column(String(100),  nullable=False)
    score      = Column(DECIMAL(3,1), nullable=False)
    quote      = Column(String(500))
    updated_at = Column(TIMESTAMP, server_default='CURRENT_TIMESTAMP',
                        onupdate='CURRENT_TIMESTAMP')

Base.metadata.create_all(engine)          # 表若不存在则自动建
Session = sessionmaker(bind=engine)
session = Session()

# 3. 抓取 + 增量写入
headers = {'User-Agent': 'Mozilla/5.0'}
for start in tqdm(range(0, 250, 25), desc='抓取并持久化'):
    url = f'https://movie.douban.com/top250?start={start}'
    html = requests.get(url, headers=headers, timeout=10).text
    soup = BeautifulSoup(html, 'html.parser')

    for item in soup.select('div.item'):
        rank = int(item.em.text)
        title = item.a.img['alt']
        score = float(item.select_one('.rating_num').text)
        quote_tag = item.select_one('.inq')
        quote = quote_tag.text if quote_tag else ''

        # 4. 核心：存在就更新，不存在就插入（UPSERT）
        movie = session.merge(MovieTop250(rank250=rank, title=title,
                                          score=score, quote=quote))
        # merge 会先查唯一键 uk_rank，有则更新，无则插入
    time.sleep(1.5)          # 礼貌爬取

session.commit()
session.close()
print('全部写完，MySQL 已增量更新/插入完成！')