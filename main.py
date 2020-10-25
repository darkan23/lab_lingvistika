import sqlite3
import datetime

from timeloop import Timeloop
from datetime import timedelta
from datetime import datetime
from urllib import request
from config import settings
from bs4 import BeautifulSoup
from flask import Flask, render_template

timer = Timeloop()
app = Flask(__name__)

create_table_connect = sqlite3.connect("resource/news.db")
create_table_cursor = create_table_connect.cursor()
create_table_cursor.execute("""
CREATE TABLE IF NOT EXISTS news 
    (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        name TEXT NOT NULL, 
        text TEXT NOT NULL,
        link_topic TEXT NOT NULL,
        date_publication DATETIME NOT NULL 
    )
""")


@app.route('/')
def main():
    result = []
    with sqlite3.connect("resource/news.db") as conn:
        cur = conn.cursor()
        cur.execute("SELECT name, text, link_topic, date_publication FROM news")
        result_select = cur.fetchall()
        if result_select is not None:
            for item in result_select:
                result.append([item[0], item[1], item[2], item[3]])
        else:
            result.append(['Данные отсутсвуют', ':с', 'отсутсвуют', 'error'])

    return render_template('main.html', data=result)


@timer.job(interval=timedelta(hours=settings['hours_step']))
def update_planted():
    log('Начало выполнения job-ы')
    main_html = get_html('https://novostivolgograda.ru/news')
    if main_html is not None:
        links_topics = get_links_topics(main_html)
        log('Получено {} записей'.format(len(links_topics)))
        for link in links_topics:
            topic_html = get_html(link)
            date_time, name, text = parse_topic(topic_html)
            value = (None, name, text, link, time_parse(date_time))
            with sqlite3.connect("resource/news.db") as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM news WHERE name = ? AND link_topic = ?", (name, link))
                result = cur.fetchone()
                if result is None:
                    cur.execute("""INSERT INTO news VALUES (?, ?, ?, ?, ?)""", value)
                    conn.commit()
                    log('Вставлена новая запись |{}, {}'.format(name[:50] + '...', link[:50] + '...'))
                else:
                    log('Запись уже существует |{}'.format(name[:100] + '...'))


timer.start(block=False)


def get_html(url: str):
    log('Получение html c {}'.format(url))
    query = request.urlopen(url)
    code_response = query.getcode()
    if code_response == 200:
        soup = BeautifulSoup(query.read(), 'html.parser')
        return str(soup)
    else:
        log('Code response is {}'.format(code_response))
        return None


def get_links_topics(html: str):
    log('Получение ссылок на топики')
    result = []
    soup = BeautifulSoup(html, "html.parser")
    a_topics = soup.find_all('a', class_='matter')
    for topic in a_topics:
        result.append('https://novostivolgograda.ru{}'.format(topic['href']))
    return result


def parse_topic(html: str):
    log('Получение данных о топике')
    soup = BeautifulSoup(html, "html.parser")
    span_topic_time = soup.find('div', class_='meta').find('span')
    div_topic_name = soup.find('div', class_='cm-subtitle')
    div_topic_text = soup.find('div', class_='content-blocks')
    return span_topic_time.text, div_topic_name.text, div_topic_text.text


def time_parse(time_str: str):
    date, time_value = time_str.split(', ')
    if date == 'Сегодня':
        full_date_str = '{} {}'.format(datetime.date(datetime.now()), time_value)
        convert_time = datetime.strptime(full_date_str, '%Y-%m-%d %H:%M')
    else:
        convert_time = datetime.date(datetime.now())
    return convert_time


def log(log_info_str):
    print('[{}] - {}'.format(datetime.now(), log_info_str))


if __name__ == '__main__':
    app.run()
