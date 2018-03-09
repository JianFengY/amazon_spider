"""
Created on 2018/3/9
@Author: Jeff Yang
"""
import requests
from requests.exceptions import RequestException
from pyquery import PyQuery as pq
import pymongo
from multiprocessing import Pool
import re

client = pymongo.MongoClient('localhost')
db = client['amazon']

start_url = "https://www.amazon.com/Best-Sellers/zgbs/ref=zg_bsnr_tab"

base_url = "https://www.amazon.com"


def get_page_html(url):
    """获取页面的源码"""
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException as e:
        print(e.message)


def get_page_detail(html):
    """获取一页中的商品信息"""
    pattern = re.compile('<div class="zg_itemImmersion".*?<span class="zg_rankNumber">\s*(\d+).*?'
                         + '<a class="a-link-normal" href="(.*?)">.*?'
                         + '<img.*?src="(.*?)".*?'
                         + '<div class="p13n-sc-truncate.*?>(.*?)</div>.*?'
                         + '<span class="a-icon-alt">(.*?)</span>.*?'
                         + '<span class=\'p13n-sc-price\'>(.*?)</span>', re.S)

    items = re.findall(pattern, html)
    for item in items:
        yield {
            'rank': item[0],
            'url': base_url + item[1],
            'img_url': item[2],
            'title': item[3].strip(),
            'star': item[4],
            'price': item[5],
        }


def save_to_mongo(data):
    """保存到MongoDB"""
    if db['products'].insert(data):
        print('Save to Mongo: ', data['title'])
    else:
        print('Save to Mongo failed: ', data['title'])


def main():
    pool = Pool()
    start_page = get_page_html(start_url)
    doc = pq(start_page)
    items_links = doc('.zg_homeWidget').items()
    for items_link in items_links:
        url = items_link('.zg_homeListLink a').attr('href')
        # 每种商品都有5页，这里构造出所有URL
        urls = [url + '/ref=zg_bs_pg_1?_encoding=UTF8&pg=' + str(i) for i in range(1, 6)]
        # pool.map(get_page_html, [url for url in urls])
        for url in urls:
            html = get_page_html(url)
            for item in get_page_detail(html):
                save_to_mongo(item)


if __name__ == '__main__':
    main()
