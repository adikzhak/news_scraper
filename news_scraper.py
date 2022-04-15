import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import dateparser
import time
from tldextract import extract
import warnings

# Функция для очистки текста от тэгов 

def merge_contents(data):
    tags = re.compile(r'<.*?>')
    return re.sub(tags, '', data)

# Функция для взятия домена из URL
def get_domain(url):    
    subdomain, domain, suffix = extract(url)
    return 'http://' + domain + '.' + suffix

# Функция для присвоения элементов списка элементам словаря
def arr_to_dict(arr):
    res = {'resource_id': '', 'resource_url': '', 'top_tag': '', 'bottom_tag': '', 'title_cut': '', 'date_cut': ''}
    res['resource_id'] = arr[0]
    res['resource_url'] = arr[1]
    res['top_tag'] = arr[2]
    res['bottom_tag'] = arr[3]
    res['title_cut'] = arr[4]
    res['date_cut'] = arr[5]
    return res

# Фугкция для добавления строк в таблицу resource
def complete_res(resource_url, top_tag, bottom_tag, title_cut, date_cut):
    r = requests.get(resource_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    resource_name = soup.title.text
    cur.execute(f"INSERT INTO resource (resource_name, resource_url, top_tag, bottom_tag, title_cut, date_cut) VALUES('{resource_name}', '{resource_url}', '{top_tag}', '{bottom_tag}', '{title_cut}', '{date_cut}');")
    con.commit()

# Элементы таблицы resource в виде списка словарей 
def all():
    res = []
    cur.execute("SELECT resource_id, resource_url, top_tag, bottom_tag, title_cut, date_cut FROM resource")
    for row in cur.fetchall():
        res.append(arr_to_dict(list(row)))
    return res

# Парсер новостей
def collect_news():
    print('Дождитесь завершения парсинга!')

    for data in all():
        r = requests.get(data['resource_url'])
        soup = BeautifulSoup(r.text, 'html.parser')
        html_news = soup.find_all(class_= data['top_tag'])
        allinks = []
        for row in html_news:
            allinks.append(row.find('a').get('href'))
        for link in allinks:
            if link[0] == '/':
                link = get_domain(data['resource_url']) + link
            req = requests.get(link)
            soup = (BeautifulSoup(req.text, 'html.parser'))
            res_id = data['resource_id']
            title = soup.find(data['title_cut']).text
            try:
                date = dateparser.parse(soup.find(class_ = data['date_cut']).text)            
            except:
                try:
                   date = dateparser.parse(soup.find('main').find(data['date_cut']).text)
                except AttributeError:
                   pass
                           
            content = soup.find_all(data['bottom_tag'])
            nd_date = int(time.mktime(time.strptime(str(date), '%Y-%m-%d %H:%M:%S')))
            s_date = int(time.time())
            cur.execute("INSERT INTO items (res_id, link, title, content, nd_date, s_date, not_date) VALUES(?, ?, ?, ?, ?, ?, ?)", (res_id, link, title, merge_contents(' '.join(str(i) for i in content)), nd_date, s_date, date))
            con.commit()
                  
         

    print('Парсинг новостей завершён!')


if __name__ == "__main__":

    warnings.filterwarnings(
       "ignore",
       message="The localize method is no longer necessary, as this time zone supports the fold attribute",
    )

    con = sqlite3.connect('sqlite.db')
    cur = con.cursor()
    cur.executescript("""CREATE TABLE IF NOT EXISTS resource(
       resource_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
       resource_name VARCHAR(255),
       resource_url VARCHAR(255),
       top_tag VARCHAR(255) NOT NULL,
       bottom_tag VARCHAR(255) NOT NULL,
       title_cut VARCHAR(255) NOT NULL,
       date_cut VARCHAR(255) NOT NULL);
   
       CREATE TABLE IF NOT EXISTS items(
       id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
       res_id INT NOT NULL,
       link VARCHAR(255) NOT NULL,
       title TEXT NOT NULL,
       content TEXT NOT NULL,
       nd_date INT NOT NULL,
       s_date INT NOT NULL,
       not_date DATE NOT NULL,
       FOREIGN KEY (res_id)  REFERENCES resource (resource_id));
    """)
    con.commit()   
 
    collect_news()
   