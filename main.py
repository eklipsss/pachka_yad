
from pachka_api import send_message, get_chat_info, hello_users
from postgreSQL import (get_elem_info, add_url, add_chat, add_elem, add_link, get_track_elems_by_chat,
                        url_exists, chat_exists, elem_exists, link_exists, make_public_elem,
                        delete_link, get_parent_track_elems, get_chats_by_elem,
                        change_name_elem, get_children, delete_tree, url_activate, code_url, on_startup)
from yad_api import Yad_client
from pprint import pprint
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional


app = FastAPI()
scheduler = AsyncIOScheduler()

users_requests = {}


@app.get("/")
def hello():
    return {"ping": "pong!"}


class WebhookEvent(BaseModel):
    type: str
    id: int
    event: str
    entity_type: str
    entity_id: int
    content: str
    user_id: int
    created_at: str
    chat_id: int
    thread: Optional[dict] = None


@app.post("/webhook")
async def hook(event: WebhookEvent):
    event.entity_type = 'discussion'
    print('EVENT: ', event)
    if event.content[0] == '/':

        if event.content == '/all_tracking':
            await print_all_elements(event.chat_id, event.entity_type)

        elif event.content == '/add_tracking':
            await add_tracking(event.chat_id, event.entity_type)

            # любая команда всегда становится на первый шаг и перекрывает предыдущую, если таковая есть
            users_requests[(event.chat_id, event.user_id)] = {1: ('/add_tracking', 0)}

        elif event.content == '/delete_tracking':
            dict = await delete_tracking(event.chat_id, event.user_id, event.entity_type)
            # любая команда всегда становится на первый шаг и перекрывает предыдущую, если таковая есть
            users_requests[(event.chat_id, event.user_id)] = {1: ('/delete_tracking', dict)}
            print(users_requests)

        elif event.content == '/done':
            pop = users_requests.pop((event.chat_id, event.user_id), None)
            if pop:
                text = ("Работа с командой завершена")
                await send_message(event.chat_id, text, event.entity_type)

        elif event.content == '/help':
            text = (
                "Привет! Я бот, который поможет тебе получать уведомления о различных событиях, происходящих с элементами Ядиска\n\n"
                "**Команды:** \n"
                "  ✏️  /all_tracking - показать все элементы, на отслеживание которых ты подписан \n"
                "  ✏️  /add_tracking - добавить трекинг элемента \n"
                "  ✏️  /delete_tracking - удалить трекинг элемента \n"
                "  ✏️  /help - посмотреть команды \n")
            await send_message(event.chat_id, text, event.entity_type)

    else:
        print('else')
        print(users_requests)
        if (event.chat_id, event.user_id) in users_requests:
            print('if')
            await add_request(event.chat_id, event.entity_type, event.user_id, event.content)

    return "Hello World!"


yad = Yad_client()
start_text = 'Уведомление: \n'


async def add_request(chat_id, entity_type, user_id, content):
    print('add_request')

    dict = users_requests[(chat_id, user_id)]
    i = len(dict)
    dict[i+1] = content
    users_requests[(chat_id, user_id)] = dict
    if dict[1][0] == '/add_tracking':
        await add_tracking2(content, chat_id, entity_type)
    elif dict[1][0] == '/delete_tracking':
        await delete_tracking2(chat_id, content, dict[1][1], entity_type)


async def print_all_elements(chat_id, entity_type):
    elems = await get_track_elems_by_chat(chat_id)
    text = "Список элементов, на трекинг которых вы подписаны:\n\n"
    i = 1
    dict = {}
    if elems:
        for elem in elems:
            text += f"{i}) {elem['url']}\n"
            dict[str(i)] = elem
            i += 1

    bot_message = await send_message(chat_id, text, entity_type)
    return dict


async def add_tracking(chat_id, entity_type):
    print('add_tracking')

    text = ("Для добавления трекинга элемента Я.диска укажите url публичного ресурса \n"
            "Для завершения работы с командой - /done")
    bot_message = await send_message(chat_id, text, entity_type)


async def add_tracking2(content, chat_id, entity_type):
    print('add_tracking2')

    url = content
    if url.find("https://disk.yandex.ru", 0, 22) == 0:
        res_info = await yad.disk_info_public_res(url)

        if not res_info.get('error'):
            text = (f"Трекинг элемента {url} добавлен успшено\n\n"
                    f"Для добавления трекинга следующего ресурса отправьте url нового публичного ресурса  \n"
                    f"Для завершения работы с командой - /done")
            await add_track_res(res_info, chat_id, entity_type)
        else:
            text = (f"Трекинг элемента {url} не может быть добавлен: получен неверный url\n\n"
                    f"Для добавления трекинга отправьте url публичного ресурса повторно  \n"
                    f"Для завершения работы с командой - /done")
            
    else:
        text = (f"Трекинг элемента {url} не может быть добавлен: получен неверный url\n\n"
                f"Для добавления трекинга отправьте url публичного ресурса повторно  \n"
                f"Для завершения работы с командой - /done")
    bot_message = await send_message(chat_id, text, entity_type)


async def add_track_res(res_info, chat_pachka_id, entity_type):
    print('add_track_res')

    url = res_info['public_url']
    url_id = await url_exists(url)
    new_url = 0
    if url_id == -1:
        url_id = await url_activate(url)
    elif url_id == 0:
        url_id = await add_url(url)
        new_url = 1

    chat = await get_chat_info(chat_pachka_id)
    name = chat['name']
    chat_id = await chat_exists(chat_pachka_id=chat_pachka_id)
    if not chat_id:
        if entity_type != 'user':
            entity_type = 'chat'
        chat_id = await add_chat(chat_pachka_id, name, entity_type)

    elem_id = await elem_exists(res_info['resource_id'])
    if not elem_id:
        elem_id = await add_elem(res_info['name'], res_info['type'], 0, url_id, res_info['resource_id'])

        contents = await yad.disk_info_public_res(url, '/')
        pprint(contents)

        if '_embedded' in contents:
            for item in contents['_embedded']['items']:
                await rec_insert(item, url, '/', elem_id, url_id)

    else:
        if new_url:
            await make_public_elem(res_info['resource_id'], url_id)

    link_id = await link_exists(chat_id, elem_id)
    if not link_id:
        link_id = await add_link(chat_id, elem_id)
        print('link_id', link_id)


async def rec_insert(item, public_url, path='/', parent_id=None, url_id=None):
    print('rec_insert')
    if path == '/':
        path = path + await code_url(item['name'])
    else:
        path = path + '/' + await code_url(item['name'])

    res_info = await yad.disk_info_public_res(public_url, path)
    if not res_info.get('error'):
        elem_id = await add_elem(res_info['name'], res_info['type'], parent_id, url_id, res_info['resource_id'], path)

        if '_embedded' in res_info:
            for item in res_info['_embedded']['items']:
                await rec_insert(item, public_url, path, elem_id, url_id)
    else:
        print('что-то не так')


async def delete_tracking(chat_id, user_id, entity_type):
    print('delete_tracking')

    dict = await print_all_elements(chat_id, entity_type)
    text = ("Для удаления трекинга укажите один номер элемента (без скобочки) из приведенного выше списка\n"
            "Для завершения работы с командой - /done")
    await send_message(chat_id, text, entity_type)
    return dict
    
    
async def delete_tracking2(chat_id, content, dict, entity_type):
    print('delete_tracking2')

    i = content
    elem = dict.get(i)
    if elem:
        await delete_link(chat_id, elem['res_id'])

        text = (f"Трекинг элемента {elem['url']} удален \n\n"
                f"Для удаления следующего трекинга укажите один номер элемента (без скобочки) из приведенного выше списка\n"
                f"Для завершения работы с командой - /done")
    else:
        text = (f"Введен несуществующий номер \n\n"
                f"Для удаления трекинга укажите один номер элемента (без скобочки) из приведенного выше списка повторно\n"
                f"Для завершения работы с командой - /done")
    await send_message(chat_id, text, entity_type)


def format_url(path):
    path = path.replace(' ', '%20')
    return path


async def tracking_change():
    date = datetime.now()
    date = (date - timedelta(hours=3, minutes=5))

    elems = await get_parent_track_elems()
    chats = []
    dict = {}

    for elem in elems:

        # получаем все чаты, подписанные на обновления данного элемента
        cur_chats = await get_chats_by_elem(elem['elem_id'])

        # добавляем чаты в массив + добавляем в словарь связку: элемент - количество добавленных чатов
        i = 0
        for chat in cur_chats:
            if not chat['chat_pachka_id'] in chats:
                chats.append(chat['chat_pachka_id'])
                i += 1
        dict[elem['elem_id']] = i

        # получаем инфу об этом элементе с ядиска
        res_info = await yad.disk_info_public_res(elem['url'])
        if not res_info.get('error'):
            # проверяем, изменилось ли имя
            if res_info['name'] != elem['name']:
                form_path = format_url(elem['path'])
                text = f"Имя ресурса {elem['url'] + form_path} было изменено:  {elem['name']} --> {res_info['name']}"
                await change_name_elem(elem['elem_id'], res_info['name'], elem['url'], '/')
                elem = await get_elem_info(elem['elem_id'])
                elem = elem[0]
                for chat in chats:
                    await send_message(chat, text)

            await check_elem_change(elem, chats, dict, res_info, date)

        else:
            text = f"Публичный ресурс <{elem['name']}> был удален"

            for chat in chats:
                await send_message(chat, text)

            await delete_tree(elem['elem_id'])

        for i in range(dict[elem['elem_id']]):
            chats.pop()


async def check_elem_change(elem, chats, dict, res_info, date):
    print('---------------------------------------')
    print('check_elem_change')

    url = elem['url']

    if elem['type'] == 'file':

        res_date = datetime.strptime(res_info['modified'], '%Y-%m-%dT%H:%M:%S+00:00')

        if res_date >= date:
            form_path = format_url(elem['path'])
            text = f"Содержимое файла {elem['url'] + form_path} было изменено"
            for chat in chats:
                await send_message(chat, text)
    else:
        form_path = format_url(elem['path'])
        res_info = await yad.disk_info_public_res(elem['url'], form_path)
        if not res_info.get('error'):
            cur_child_elems = res_info['_embedded']['items']
            child_elems = await get_children(elem['elem_id'])

            for i in range(len(child_elems)):
                child_elems[i] = child_elems[i]['res_id']

            for cur_child_elem in cur_child_elems:
                if not cur_child_elem['resource_id'] in child_elems:
                    form_path = format_url(elem['path'])
                    form_path_child = format_url(cur_child_elem['path'])
                    text = f"В папку {elem['url'] + form_path} добавлен элемент {elem['url'] + form_path_child}"
                    url_id = await url_exists(url)
                    await rec_insert(url, cur_child_elem['path'], elem['elem_id'], url_id)

                    for chat in chats:
                        await send_message(chat, text)

            path = elem['path']

            child_elems = await get_children(elem['elem_id'])

            for child_elem in child_elems:

                # получаем все чаты, подписанные на обновления данного элемента
                cur_chats = await get_chats_by_elem(child_elem['elem_id'])

                # добавляем чаты в массив + добавляем в словарь связку: элемент - количество добавленных чатов
                i = 0
                for chat in cur_chats:
                    if not chat['chat_pachka_id'] in chats:
                        chats.append(chat['chat_pachka_id'])
                        i += 1
                dict[child_elem['elem_id']] = i

                for cur_child_elem in cur_child_elems:
                    # проверяем, не изменилось ли название
                    if cur_child_elem['resource_id'] == child_elem['res_id']:
                        if cur_child_elem['name'] != child_elem['name']:
                            form_path = format_url(child_elem['path'])
                            text = f"Имя ресурса {child_elem['url'] + form_path} было изменено:  {child_elem['name']} --> {cur_child_elem['name']}"
                            if path != '/':
                                await change_name_elem(child_elem['elem_id'], cur_child_elem['name'], url, path + '/' + cur_child_elem['name'])
                            else:
                                await change_name_elem(child_elem['elem_id'], cur_child_elem['name'], url, path + cur_child_elem['name'])
                            child_elem = await get_elem_info(child_elem['elem_id'])
                            child_elem = child_elem[0]
                            for chat in chats:
                                await send_message(chat, text)

                form_path = format_url(child_elem['path'])
                res_info = await yad.disk_info_public_res(child_elem['url'], form_path)
                if not res_info.get('error'):
                    await check_elem_change(child_elem, chats, dict, res_info, date)

                else:
                    text = f"Публичный ресурс <{child_elem['name']}> был удален"

                    for chat in chats:
                        await send_message(chat, text)
                    await delete_tree(child_elem['elem_id'])

                for i in range(dict[child_elem['elem_id']]):
                    chats.pop()
        else:
            text = f"Публичный ресурс <{elem['name']}> был удален"

            for chat in chats:
                await send_message(chat, text)

            await delete_tree(elem['elem_id'])


@app.on_event("startup")
async def startup_event():
    await on_startup()


scheduler.add_job(hello_users, 'interval', seconds=15)
scheduler.add_job(tracking_change, 'interval', minutes=5)
scheduler.start()