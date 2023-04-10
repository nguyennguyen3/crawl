import time
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import psycopg2


def get_connect():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="localhost",
                                      port="5432",
                                      database="crawl")

        cursor = connection.cursor()
    except:
        connection = None
        cursor = None
    return connection, cursor


def crawl_list(category):
    connection, cursor = get_connect()

    page = 1
    while 1:
        url = f'https://chiaki.vn/' \
              f'{category}' \
              f'?page={page}'
        try:
            time.sleep(1)
            response = requests.get(url)
            print(f"Crawl {category} : {page}")
            data = BeautifulSoup(response.text, 'html.parser')
            if 'Dữ liệu bạn quan tâm chưa có trên hệ thống!' not in data:
                res = data.find("div", {"class": "list-product-contain"}). \
                    find_all("div", {"class": "col-md-3 product-item col-sm-3 col-xs-6 ids-get-pos"})
                for ele in res:
                    ele = ele.find("h3", {"class": "product-title"}).find("a")
                    uri = f"https://chiaki.vn{ele.get('href')}"
                    name = ele.get("title").lower()
                    postgres_insert_query = """ INSERT INTO list (product, url) VALUES (%s,%s)"""
                    record_to_insert = (name, uri)
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
            else:
                break
            page += 1
        except:
            print(response.status_code)
            break
    cursor.close()
    connection.close()
    print(f"Crawl done {category}")


def crawl_detail(url, connection, cursor):
    time.sleep(1)
    response = requests.get(url)
    data = BeautifulSoup(response.text, 'html.parser')
    if 'Xin lỗi!  Trang bạn tìm không có trong hệ thống!' not in data:
        name = data.find("span", {"id": "js-product-title"}).get_text().lower()
        brand = data.find_all("a", {"id": "js-product-manufacturer"})[0].get_text().lower()
        detail = data.find("div", {"id": "content-product"})
        description = ""
        for ele in detail:
            description += f" {ele.get_text().strip().lower()}"
        postgres_insert_query = """ INSERT INTO product (name, brand, description) VALUES (%s,%s,%s)"""
        record_to_insert = (name, brand, description.strip())
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # file_path = "chiaki.txt"
    # categories = []
    # with open(file_path) as file:
    #     for line in file:
    #         line = line.strip()
    #         categories.append(line)
    # for category in categories:
    #     crawl_list(category)
    connection, cursor = get_connect()
    postgres_insert_query = """select url from list"""
    cursor.execute(postgres_insert_query)
    records = cursor.fetchall()
    length = len(records)
    for i in tqdm(range(length)):
        crawl_detail(records[i][0], connection, cursor)
    cursor.close()
    connection.close()
