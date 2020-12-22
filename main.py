import requests
from bs4 import BeautifulSoup
import csv
from urllib.parse import urlparse
import pymorphy2
import nltk
from nltk.corpus import stopwords
import threading
from queue import Queue
queue = Queue()
LOCK = threading.RLock()
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
Thread_list = []


def pages_append(url, title, description):
    LOCK.acquire()
    Pages.append({
        'id': Page_id[0],
        'url': url,
        'title': title,
        'description': description,
    })
    Page_id.insert(0, Page_id[0] + 1)
    LOCK.release()


def quantity_append(word, page, domain):
    LOCK.acquire()
    flag = 0
    for d_word in Domain_quantity:
        if word == d_word['word']:
            d_word['quantity'] += 1
            flag = 1
            break
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
    LOCK.release()


def words_append(word, page, domain):
    LOCK.acquire()
    flag = 1
    for letter in word:
        if letter not in l_allowed:
            flag = 0
            break
    if flag == 1:
        word = morph.parse(word)[0].normal_form
        if word not in en_stops:
            Words.append({
                'id': Word_id[0],
                'word': word,
            })
        quantity_append(word, page, domain)
        Word_id.insert(0, Word_id[0] + 1)
    LOCK.release()
    return flag


def page_word_append(page_id, word_id):
    LOCK.acquire()
    Page_Word.append({
        'page id': page_id,
        'word id': word_id,
    })
    LOCK.release()


def link_word_page_append(page_id, word_id):
    LOCK.acquire()
    Link_Word_Page.append({
        'page id': page_id,
        'word id': word_id,
    })
    LOCK.release()


def thread_start(target, args):
    t = threading.Thread(target=target, args=args)
    Thread_list.append(t)
    t.start()


def thread_limit():
    for t in Thread_list:
        t.join()
    Thread_list.clear()


def get_html(url):
    try:
        request = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(request.text, 'html.parser')
        return soup
    except:
        return None


def absolute_link(link, host, domain):
    if all(not link.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
        # проверяем, является ли ссылка относительной
        if link.startswith('/') and not link.startswith('//'):
            # преобразуем относительную ссылку в абсолютную
            link = domain + link
        if host not in link:
            link = host + link
    return link


def check_link_to_replay(link):
    flag = 1
    for page in Pages:
        if page['url'] == link:
            flag = 0
            break
    return flag


def add_all_links(url, host, domain, threads_count=1, max_depth=0):
    soup = get_html(url)
    if soup:
        for tag_a in soup.find_all('a', href=True):
            link = tag_a.get('href')
            link = absolute_link(link, host, domain)
            if urlparse(link).netloc == domain and check_link_to_replay(link):
                pages_append(link, 'No title', 'No description')
                if max_depth != 0:
                    queue.put(link)

    if max_depth != 0:
        while queue.qsize() >= threads_count:
            thread_limit()
            for i in range(threads_count):
                target_link = queue.get_nowait()
                thread_start(target=add_all_links, args=(target_link, host, domain, threads_count, 0))
        while queue.qsize():
            thread_limit()
            target_link = queue.get_nowait()
            thread_start(target=add_all_links, args=(target_link, host, domain, threads_count, 0))


def add_title_description(soup, page):
    title = soup.find('title')
    if title:
        page['title'] = title.get_text()
    description = soup.find('meta', {'name': 'description'})
    if description:
        page['description'] = description['content']


def add_page_info(page, host, domain):
    soup = get_html(page['url'])
    if soup:
        add_title_description(soup, page)
        for tag_a in soup.find_all('a', href=True):
            link = tag_a.get('href')
            link = absolute_link(link, host, domain)
            if urlparse(link).netloc == domain:
                link_page_id = 0
                for element in Pages:
                    if element['url'] == link:
                        link_page_id = element['id']
                        break
                link_sentence = tag_a.get_text()
                link_words = link_sentence.split()
                for word in link_words:
                    LOCK.acquire()
                    word = words_append(word, page, domain)
                    if word:
                        page_word_append(page['id'], Word_id[0])
                        link_word_page_append(link_page_id, Word_id[0])
                    LOCK.release()
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
                        LOCK.acquire()
                        word = words_append(word, page, domain)
                        if word:
                            page_word_append(page['id'], Word_id[0])
                        LOCK.release()


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


def get_user_info():
    url = input('Введите url сайта: ')
    deep = input('Введите глубину просмотра сайта: ')
    file = input('Введите путь к файлу: ')
    threads_count = int(input('Введите количество потоков обработки сайта: '))
    user_info = {
        'url': url.strip(),
        'host': url.split('/')[0] + '//',
        'domain': url.split('/')[2],
        'deep': deep,
        'file': file,
        'threads_count': int(threads_count),
    }
    return user_info


def main():
    user_info = get_user_info()

    if user_info['deep'] == '0':
        print('please wait...')
        pages_append(user_info['url'], 'No title', 'No description')
        add_all_links(user_info['url'], user_info['host'], user_info['domain'], max_depth=0)
        print(f'Получено {len(Pages)} страниц')

        print(f'Обработка {user_info["url"]} 1 страницы из 1')
        add_page_info(Pages[0], user_info['host'], user_info['domain'])

    else:
        print('please wait...')
        pages_append(user_info['host'] + user_info['domain'] + '/', 'No title', 'No description')
        add_all_links(user_info['host'] + user_info['domain'], user_info['host'], user_info['domain'], user_info['threads_count'], max_depth=1)
        thread_limit()
        print(f'Получено {len(Pages)} страниц')

        if user_info['threads_count'] > len(Pages):
            user_info['threads_count'] = input(f'Введите число потоков меньшее {len(Pages)}: ')

        print('Запуск потоков...')
        for pages in range(0, len(Pages), user_info['threads_count']):
            if (user_info['threads_count'] + pages) <= len(Pages):
                for i in range(user_info['threads_count']):
                    print(f'обработка {Pages[pages + i]["url"]} {Pages[pages + i]["id"]} страница из {len(Pages)}')
                    thread_start(target=add_page_info, args=(Pages[pages + i], user_info['host'], user_info['domain']))
                thread_limit()

        remaining = len(Pages) - (len(Pages) % user_info['threads_count'])
        for i in range(remaining, len(Pages)):
            print(f'обработка {Pages[i]["url"]} {Pages[i]["id"]} страница из {len(Pages)}')
            add_page_info(Pages[i], user_info['host'], user_info['domain'])

    save_file(user_info['file'])

    print('success')


main()
