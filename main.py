import requests
from bs4 import BeautifulSoup
import csv
from urllib.parse import urlparse
import pymorphy2
import nltk
from nltk.corpus import stopwords
import threading
nltk.download('stopwords')
en_stops = set(stopwords.words('english'))
morph = pymorphy2.MorphAnalyzer()


HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36', 'accept': '*/*'}
FORBIDDEN_PREFIXES = ['#', 'tel:', 'mailto:']
Pages = []
Words = []
Page_Word = []
Link_Word_Page = []
Domain_quantity = []
Page_quantity = []
Page_id = [1]
Word_id = [1]
l_allowed = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюяabcdefghigklmnopqrstuvwxyz'


def get_html(url):
    try:
        request = requests.get(url, headers=HEADERS)
        return request
    except:
        return None


def add_all_links_recursive(url, host, domain, max_depth=0):
    # список ссылок, от которых в конце мы рекурсивно запустимся
    links_to_handle_recursive = []

    # получаем html код страницы
    request = get_html(url)
    # парсим его с помощью BeautifulSoup
    if request:
        try:
            soup = BeautifulSoup(request.text, 'html.parser')
            # рассматриваем все теги <a>
            for tag_a in soup.find_all('a', href=True):
                # получаем ссылку, соответствующую тегу
                link = tag_a.get('href')
                # если ссылка не начинается с одного из запрещённых префиксов
                if all(not link.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
                    # проверяем, является ли ссылка относительной
                    if link.startswith('/') and not link.startswith('//'):
                        # преобразуем относительную ссылку в абсолютную
                        link = domain + link
                    if host not in link:
                        link = host + link
                    # проверяем, что ссылка ведёт на нужный домен
                    # и что мы ещё не обрабатывали такую ссылку
                    if urlparse(link).netloc == domain:
                        flag = 0
                        for page in Pages:
                            if page['url'] == link:
                                flag = 1
                                break
                        if flag == 0:
                            Pages.append({
                                'id': Page_id[0],
                                'url': link,
                                'title': 'No title',
                                'description': 'No description',
                            })
                            Page_id.insert(0, Page_id[0] + 1)
                            links_to_handle_recursive.append(link)
        except:
            pass

    if max_depth == 0:
        return
    for link in links_to_handle_recursive:
        add_all_links_recursive(link, host, domain)


def add_page_info(page, host, domain):
    html = get_html(page['url'])
    if html:
        try:
            soup = BeautifulSoup(html.text, 'html.parser')
            title = soup.find('title')
            if title:
                page['title'] = title.get_text()
            description = soup.find('meta', {'name': 'description'})
            if description:
                page['description'] = description['content']
            for tag_a in soup.find_all('a', href=True):
                link = tag_a.get('href')
                if all(not link.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
                    if link.startswith('/') and not link.startswith('//'):
                        link = host + domain + link
                    if urlparse(link).netloc == domain:
                        for element in Pages:
                            if element['url'] == link:
                                break
                        try:
                            link_sentence = tag_a.get_text()
                            link_words = link_sentence.split()
                            for word in link_words:
                                flag = 0
                                for letter in word:
                                    if letter not in l_allowed:
                                        flag = 1
                                        break
                                if flag == 0:
                                    word = morph.parse(word)[0].normal_form
                                    if word not in en_stops:
                                        Words.append({
                                            'id': Word_id[0],
                                            'word': word,
                                        })
                                        Page_Word.append({
                                            'page id': page['id'],
                                            'word id': Word_id[0],
                                        })
                                        Link_Word_Page.append({
                                            'page id': element['id'],
                                            'word id': Word_id[0],
                                        })
                                        flag = 0
                                        for d_word in Domain_quantity:
                                            if word == d_word['word']:
                                                d_word['quantity'] += 1
                                                flag = 1
                                        if flag == 0:
                                            Domain_quantity.append({
                                                'page id': domain,
                                                'word': word,
                                                'quantity': 1,
                                            })
                                        flag = 0
                                        for p_word in Page_quantity:
                                            if page['url'] == p_word['page id'] and word == p_word['word']:
                                                p_word['quantity'] += 1
                                                flag = 1
                                        if flag == 0:
                                            Page_quantity.append({
                                                'page id': page['url'],
                                                'word': word,
                                                'quantity': 1,
                                            })
                                        Word_id.insert(0, Word_id[0] + 1)
                        except: pass
                tag_a.decompose()

            body = soup.find('body')
            if body:
                text = body.get_text()
                words = text.split()
                for word in words:
                    flag = 0
                    for letter in word:
                        if letter not in l_allowed:
                            flag = 1
                            break
                    if flag == 0:
                        word = morph.parse(word)[0].normal_form
                        if word not in en_stops:
                            Words.append({
                                'id': Word_id[0],
                                'word': word,
                            })
                            Page_Word.append({
                                'page id': page['id'],
                                'word id': Word_id[0],
                            })
                            flag = 0
                            for d_word in Domain_quantity:
                                if word == d_word['word']:
                                    d_word['quantity'] += 1
                                    flag = 1
                            if flag == 0:
                                Domain_quantity.append({
                                    'page id': domain,
                                    'word': word,
                                    'quantity': 1,
                                })
                            flag = 0
                            for p_word in Page_quantity:
                                if page['url'] == p_word['page id'] and word == p_word['word']:
                                    p_word['quantity'] += 1
                                    flag = 1
                            if flag == 0:
                                Page_quantity.append({
                                    'page id': page['url'],
                                    'word': word,
                                    'quantity': 1,
                                })
                            Word_id.insert(0, Word_id[0] + 1)
        except: pass


def save_file(path):
    try:
        with open(path, 'w', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(['id', 'url', 'title', 'description'])
            for item in Pages:
                try:
                    writer.writerow([item['id'], item['url'], item['title'], item['description']])
                except:
                    writer.writerow([item['id'], 'unrecognized', 'unrecognized', 'unrecognized'])
            writer.writerow(['id', 'word'])
            for item in Words:
                try:
                    writer.writerow([item['id'], item['word']])
                except:
                    writer.writerow([item['id'], 'unrecognized'])
            writer.writerow(['page id', 'page word'])
            for item in Page_Word:
                writer.writerow([item['page id'], item['word id']])
            writer.writerow(['page id', 'word id'])
            for item in Link_Word_Page:
                writer.writerow([item['page id'], item['word id']])
            writer.writerow(['domain', 'word', 'quantity'])
            for item in Domain_quantity:
                writer.writerow([item['page id'], item['word'], item['quantity']])
            writer.writerow(['page id', 'word', 'quantity'])
            for item in Page_quantity:
                writer.writerow([item['page id'], item['word'], item['quantity']])
    except:
        file = input('Введите корректный путь к файлу: ')
        save_file(file)


def parse():
    url = input('Введите url сайта: ')
    deep = input('Введите глубину просмотра сайта: ')
    file = input('Введите путь к файлу: ')
    url.strip()
    url_split = url.split('/')
    host = url_split[0] + '//'
    domain = url_split[2]

    if deep == '0':
        print('please wait...')
        Pages.append({
            'id': Page_id[0],
            'url': url,
            'title': 'No title',
            'description': 'No description',
        })
        Page_id.insert(0, Page_id[0] + 1)
        add_all_links_recursive(url, host, domain, max_depth=0)
        print(f'Получено {len(Pages)} страниц')
        print(f'Обработка {Pages[0]["url"]} 1 страницы из 1')
        add_page_info(Pages[0], host, domain)

    else:
        threads_count = int(input('Введите количество потоков обработки сайта: '))
        print('please wait...')
        Pages.append({
            'id': Page_id[0],
            'url': host + domain + '/',
            'title': 'No title',
            'description': 'No description',
        })
        Page_id.insert(0, Page_id[0] + 1)
        add_all_links_recursive(host + domain, host, domain)
        print(f'Получено {len(Pages)} страниц')
        if threads_count > len(Pages):
            threads_count = input(f'Число потоков должно быть меньше {len(Pages)}: ')
        print('Запуск потоков...')
        thread_list = []
        for page in range(0, len(Pages), threads_count):
            if (threads_count + page) < len(Pages):
                for i in range(threads_count):
                    t = threading.Thread(target=add_page_info, args=(Pages[page + i], host, domain))
                    thread_list.append(t)
                    print(f'обработка {Pages[page + i]["url"]} {Pages[page + i]["id"]} страница из {len(Pages)}')
                    t.start()
                for t in thread_list:
                    t.join()
                thread_list.clear()
        remaining = len(Pages) - (len(Pages) % threads_count)
        for i in range(remaining, len(Pages)):
            print(f'обработка {Pages[i]["url"]} {Pages[i]["id"]} страница из {len(Pages)}')
            add_page_info(Pages[i], host, domain)

    save_file(file)

    print('success')


parse()
