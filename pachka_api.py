from aiohttp import ClientSession
from postgreSQL import chat_exists, add_chat


# access_token для доступа к API, доступен пользователям с ролями "Администратор"/"Владелец", раздел "Автоматизации" -> "API"
headers = {"Authorization": "Bearer access_token"}
headers_2 = {'Content-Type': 'application/json', "Authorization": "Bearer access_token"}


# Получение списка сотрудников, добавленных в Пачку
async def get_users():
    url = "https://api.pachca.com/api/shared/v1/users"
    try:
        async with ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                users_data_array = data['data']
                print(users_data_array)
                return users_data_array
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


# Создание чата
async def create_chat(user_id, user_name):
    url = "https://api.pachca.com/api/shared/v1/messages"
    text = (f"Приветствую Вас, {user_name}!"
            f"Я твой помощник в работе с облачным хранилищем Яндекс диск."
            f"Тебе доступны следующие функции:"
            f"1) Добавление трекинга элемента(директории/файла)"
            f"2) Удаление трекинга элемента(директории/файла)"
            f"3) Просмотр элементов для трекинга"
            f"4) Изменение параметров трекинга"
            f"Для начала работы добавьте трекинг одного или нескольких элементов, для этого напишите мне номер соответствующей команды - 1")
    body = {"message": {"entity_type": "user","entity_id": user_id, "content": text}}


async def send_message(chat_id, text, entity_type='discussion'):
    print("\nsend_message")
    url = f"https://api.pachca.com/api/shared/v1/messages"
    body = {"message": {"entity_type": entity_type, "entity_id": chat_id, "content": text}}
    try:
        async with ClientSession() as session:
            async with session.post(url, headers=headers, json=body) as response:
                data = await response.json()
                data_array = data['data']
                return data_array
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


# Получение всех каналов и бесед
async def get_chats(chat_type='public', last_message_at_after='None'):
    if last_message_at_after == 'None':
        url = f"https://api.pachca.com/api/shared/v1/chats?availability={chat_type}"
    else:
        url = f"https://api.pachca.com/api/shared/v1/chats?availability={chat_type}&last_message_at_after={last_message_at_after}"
    try:
        async with ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                chats_data_array = data['data']
                return chats_data_array
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


# Получение последних сообщений чата
async def get_last_message(chat_id, per=25):
    url = f"https://api.pachca.com/api/shared/v1/messages?chat_id={chat_id}&per={per}"
    try:
        async with ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                chats_data_array = data['data']
                return chats_data_array
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


# Получение информации о конкретном сообщении
async def get_message_info(headers, message_id):
    url = f"https://api.pachca.com/api/shared/v1/messages/{message_id}"
    try:
        async with ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                chats_data_array = data['data']
                return chats_data_array
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


async def get_chat_info(chat_id):
    url = f"https://api.pachca.com/api/shared/v1/chats/{chat_id}"
    try:
        async with ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                chats_data_array = data['data']
                return chats_data_array
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


# Создание диалогов с новыми  сотрудниками
async def hello_users():
    text = (
        "Привет! Я бот, который поможет тебе получать уведомления о различных событиях, происходящих с элементами Ядиска\n\n"
        "**Команды:** \n"
        "  ✏️  /all_tracking - показать все элементы, на отслеживание которых ты подписан \n"
        "  ✏️  /add_tracking - добавить трекинг элемента \n"
        "  ✏️  /delete_tracking - удалить трекинг элемента \n"
        "  ✏️  /help - посмотреть команды \n")

    users_data_array = await get_users()
    for user_data in users_data_array:
        user_id = user_data['id']
        user_name = user_data['first_name'] + ' ' + user_data['last_name']
        # print('user_name: ', user_name)
        user_ex = await chat_exists(chat_type=str(user_id))
        if not user_ex:
            chat = await send_message(user_id, text, 'user')
            if chat:
                await add_chat(chat['chat_id'], user_name, str(user_id))

    chats_data_array = await get_chats('is_member')
    for chat_data in chats_data_array:
        chat_ex = await chat_exists(chat_pachka_id=chat_data['id'])

        if not chat_ex:
            await add_chat(chat_data['id'], chat_data['name'], 'chat')
            await send_message(chat_data['id'], text)



