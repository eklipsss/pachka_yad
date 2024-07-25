from contextlib import asynccontextmanager
import asyncpg


@asynccontextmanager
async def db_connection():
    conn = await asyncpg.connect(user='user', password='password', database='database', host='host', port=5432)
    try:
        yield conn
    finally:
        await conn.close()


async def on_startup():
    async with db_connection() as conn:
        print("Подключение к базе данных установлено")

        create_tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS public.chats (
                chat_pachka_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                chat_id SERIAL PRIMARY KEY
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.urls (
                url_id SERIAL PRIMARY KEY,
                url TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT true
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.elems (
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                parent_id INTEGER DEFAULT 0,
                elem_id SERIAL PRIMARY KEY,
                url_id INTEGER NOT NULL,
                path TEXT NOT NULL DEFAULT '/'::text,
                res_id TEXT NOT NULL,
                CONSTRAINT fk_url_id FOREIGN KEY (url_id)
                    REFERENCES public.urls (url_id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
                    NOT VALID
            );
            """,

            """
            CREATE TABLE IF NOT EXISTS public.chats_elems (
                chats_elems_id SERIAL PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                elem_id INTEGER NOT NULL,
                active BOOLEAN NOT NULL DEFAULT true,
                CONSTRAINT fk_chat_id FOREIGN KEY (chat_id)
                    REFERENCES public.chats (chat_id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
                    NOT VALID,
                CONSTRAINT fk_elem_id FOREIGN KEY (elem_id)
                    REFERENCES public.elems (elem_id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
                    NOT VALID
            );
            """
        ]

        for query in create_tables_sql:
            await conn.execute(query)


async def code_url(name):
    name = name.replace(' ', '%20')
    name = name.replace(':', '%3A')
    name = name.replace('/', '%2F')
    name = name.replace('?', '%3F')
    name = name.replace('#', '%23')
    name = name.replace('&', '%26')
    name = name.replace('=', '%3D')

    return name


async def chat_exists(chat_pachka_id=0, chat_type=''):
    async with db_connection() as conn:
        if (chat_pachka_id != 0 ) & (chat_type == 'chat'):
            result = await conn.fetchrow('SELECT chat_id FROM chats WHERE chat_pachka_id = $1 AND type = $2', chat_pachka_id, chat_type)
        elif chat_pachka_id:
            result = await conn.fetchrow('SELECT chat_id FROM chats WHERE chat_pachka_id = $1', chat_pachka_id)
        elif chat_type:
            result = await conn.fetchrow('SELECT chat_id FROM chats WHERE type = $1', chat_type)
    return result['chat_id'] if result else 0


async def elem_exists(res_id):
    async with db_connection() as conn:
        result = await conn.fetchrow('SELECT elem_id FROM elems WHERE res_id = $1', res_id)
    return result['elem_id'] if result else 0


async def url_exists(url):
    async with db_connection() as conn:
        result = await conn.fetchrow('SELECT url_id FROM urls WHERE url = $1 AND active = true', url)
        if result:
            return result['url_id']
        else:
            result = await conn.fetchrow('SELECT url_id FROM urls WHERE url = $1 AND active = false', url)
            if result:
                return -1

    return 0


async def link_exists(chat_id, elem_id, active=True):
    async with db_connection() as conn:
        result = await conn.fetchrow('SELECT chats_elems_id FROM chats_elems WHERE chat_id = $1 AND elem_id = $2 AND active = $3', chat_id, elem_id, active)

    return result['chats_elems_id'] if result else 0


async def make_public_elem(res_id, url_id):
    async with db_connection() as conn:
        result = await conn.fetchrow('UPDATE public.elems SET url_id = $1, path = \'/\' WHERE res_id = $2', url_id, res_id)
    return result


async def add_url(url, active=True):
    async with db_connection() as conn:
        result = await conn.fetchrow('INSERT INTO public.urls (url, active) VALUES ($1, $2) RETURNING url_id', url, active)
        url_id = result["url_id"]
    return url_id


async def add_chat(chat_id, name, chat_type):
    async with db_connection() as conn:
        chat_id = await conn.fetchrow('INSERT INTO chats (chat_pachka_id, name, type) VALUES ($1, $2, $3) RETURNING chat_id',
                                      chat_id, name, chat_type)
        print(f"Элемент '{name}' добавлен в базу данных")
    return chat_id['chat_id']


async def add_elem(name, elem_type, parent_id, url_id, res_id, path='/'):
    async with db_connection() as conn:

        elem_id = await conn.fetchrow('INSERT INTO elems (name, type, parent_id, url_id, path, res_id) '
                                      'VALUES ($1, $2, $3, $4, $5, $6) RETURNING elem_id',
                                      name, elem_type, parent_id, url_id, path, res_id)
        print(f"Элемент '{name}' добавлен в базу данных")
    return elem_id['elem_id']


async def add_link(chat_id, elem_id, active=True):
    async with db_connection() as conn:
        link_id = await conn.fetchrow('INSERT INTO chats_elems (chat_id, elem_id, active) VALUES ($1, $2, $3) RETURNING chats_elems_id',
                                      chat_id, elem_id, active)
    return link_id['chats_elems_id']


async def delete_link(chat_id, res_id):
    async with db_connection() as conn:
        result = await conn.fetchrow('UPDATE public.chats_elems t1 '
                                     'SET active = false '
                                     'FROM public.elems t2, public.chats t3 '
                                     'WHERE t1.elem_id = t2.elem_id '
                                     'AND t1.chat_id = t3.chat_id '
                                     'AND t3.chat_pachka_id = $1 '
                                     'AND t2.res_id = $2 '
                                     'AND t1.active = true', chat_id, res_id)
    await check_url_activity(res_id)
    return result


async def check_url_activity(res_id):
    print('check_url_activity')
    async with db_connection() as conn:
        result = await conn.fetch('SELECT * FROM chats_elems '
                                     'JOIN elems ON elems.elem_id=chats_elems.elem_id '
                                     'JOIN urls ON elems.url_id=urls.url_id '
                                     'WHERE chats_elems.active = true AND res_id = $1', res_id)
        if not result:
            result = await conn.fetch('UPDATE public.urls t1 '
                                      'SET active = false '
                                      'FROM public.elems t2 '
                                      'WHERE t1.url_id=t2.url_id '
                                      'AND t2.res_id = $1', res_id)
            elem_id = await elem_exists(res_id)
            await delete_tree(elem_id)
    return result


async def get_parent_track_elems():
    async with db_connection() as conn:
        result = await conn.fetch('select urls.url, elems.elem_id, elems.res_id, elems.name, elems.type, elems.path, elems.parent_id from elems '
                                  'join urls on elems.url_id=urls.url_id '
                                  'where urls.active = true and elems.parent_id = 0 ')
        return result


async def get_chats_by_elem(elem_id):
    async with db_connection() as conn:
        result = await conn.fetch('select chats.chat_pachka_id, chats.type from chats_elems '
                                  'join chats on chats.chat_id=chats_elems.chat_id '
                                  'where chats_elems.active = true and chats_elems.elem_id = $1', elem_id)
    return result


async def change_name_elem(elem_id, name, url, path):
    print('-------------------------------------------------------')
    print('change_name_elem')
    async with db_connection() as conn:
        elem = await get_elem_info(elem_id)
        if elem[0]['path'] == '/':
            result = await conn.fetch('update elems set name = $1 where elem_id = $2', name, elem_id)
        else:
            url_id = await url_exists(url)
            result = await conn.fetch('update elems set name = $1, url_id = $2, path = $3 where elem_id = $4', name, url_id, path, elem_id)
            print('MOM PATH: ', path)
            await change_children_path(elem_id, path)
    return result


async def change_children_path(elem_id, path):
    print('-------------------------------------------------------')
    print('change_children_path')
    async with db_connection() as conn:
        children = await get_children(elem_id)
        for child in children:
            if path != '/':
                path_child = path + '/' + await code_url(child['name'])
            else:
                path_child = path + await code_url(child['name'])
            if child['path'] != '/':
                await conn.execute('update elems set path = $1 where elem_id = $2', path_child, child['elem_id'])
            await change_children_path(child['elem_id'], path_child)


async def get_elem_info(elem_id):
    async with db_connection() as conn:
        result = await conn.fetch('select urls.url_id, urls.url, elems.elem_id, elems.res_id, elems.name, elems.type, elems.path, elems.parent_id '
                                  'from elems join urls on elems.url_id=urls.url_id '
                                  'where elems.elem_id = $1', elem_id)
    return result


async def change_url_elem(elem_id, url_id, path):
    async with db_connection() as conn:
        result = await conn.fetch('update elems set url_id = $1, path = $2 where elem_id = $3', url_id, path, elem_id)
    return result


async def delete_elem(elem_id):
    async with db_connection() as conn:
        await conn.execute('DELETE FROM chats_elems WHERE elem_id = $1', elem_id)
        await conn.execute('DELETE FROM elems WHERE elem_id = $1', elem_id)


async def delete_tree(elem_id):
    print('delete_tree')
    async with db_connection() as conn:
        children = await get_children(elem_id)
        await delete_elem(elem_id)
        for child in children:
            if child['path'] != '/':
                await delete_tree(child['elem_id'])
            else:
                result = await conn.fetch('update elems set parent_id = 0 where elem_id = $1', child['elem_id'])
                res_children = await get_children(child['elem_id'])
                if res_children:
                    for res_child in res_children:
                        await update_path(res_child, '/', child['url_id'])


async def update_path(elem, path, url_id):
    async with db_connection() as conn:
        if path != '/':
            path = path + '/' + await code_url(elem['name'])
        else:
            path = path + await code_url(elem['name'])

        if elem['path'] != '/':
            await conn.fetch('update elems set url_id = $1, path = $2 where elem_id = $3', url_id, path, elem['elem_id'])

        children = await get_children(elem['elem_id'])
        if children:
            for child in children:
                await update_path(child, path, url_id)


async def get_all_chats(chat_type='user OR chat'):
    async with db_connection() as conn:
        result = await conn.fetch('SELECT chat_pachka_id, name FROM chats WHERE type = $1', chat_type)
    return result


async def get_all_elems(elem_type='file OR dir'):
    async with db_connection() as conn:
        result = await conn.fetch('SELECT elem_id, name, public_key, parent_id FROM elems WHERE type = $1', elem_type)
    return result


async def get_children(parent_id):
    async with db_connection() as conn:
        result = await conn.fetch('select urls.url_id, urls.url, elems.elem_id, elems.res_id, elems.name, elems.type, elems.path, elems.parent_id '
                                  'from elems join urls on elems.url_id=urls.url_id '
                                  'where elems.parent_id = $1', parent_id)
    return result


async def get_track_elems_by_chat(chat_id):
    async with db_connection() as conn:
        result = await conn.fetch('SELECT urls.url, elems.res_id, chats.type '
                                  'FROM chats '
                                  'LEFT JOIN chats_elems ON chats.chat_id = chats_elems.chat_id '
                                  'LEFT JOIN elems ON chats_elems.elem_id = elems.elem_id '
                                  'LEFT JOIN urls ON elems.url_id = urls.url_id '
                                  'WHERE chats_elems.active = true AND chat_pachka_id = $1', chat_id)
    return result


async def url_activate(url):
    async with db_connection() as conn:
        result = await conn.fetch('UPDATE public.urls '
                                  'SET active = true '
                                  'WHERE url = $1', url)
        url_id = await conn.fetch('SELECT * FROM urls WHERE url = $1', url)
        print(url_id)
    return url_id['url_id']




