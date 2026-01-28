import csv, time, requests
from bs4 import BeautifulSoup
from tqdm import tqdm   # 进度条

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
csv_file = open('top250.csv', 'w', newline='', encoding='utf-8-sig')
writer = csv.writer(csv_file)
writer.writerow(['排名', '片名', '评分', '评语'])

for page in tqdm(range(0, 250, 25), desc='爬取中'):      # 每页25部，共10页
    url = f'https://movie.douban.com/top250?start={page}'
    resp = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(resp.text, 'html.parser')
    items = soup.select('div.item')
    for it in items:
        rank = it.em.text
        title = it.a.img['alt']
        score = it.select_one('.rating_num').text
        quote_tag = it.select_one('.inq')
        quote = quote_tag.text if quote_tag else ''
        writer.writerow([rank, title, score, quote])
    time.sleep(1.5)   # 礼貌爬取，别给服务器压力

csv_file.close()
print('搞定！已写入 top250.csv')