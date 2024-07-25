import aiohttp
import json


class Yad_client:
    def __init__(self):
        self.token = 'yad_token'
        self.header = {'Authorization': 'OAuth ' + self.token}
        
    async def send_query_yad(self, url_query, arr_query=None, method_query='GET'):
        if arr_query is None:
            arr_query = {}


        if method_query == 'POST':
            full_url_query = url_query
        else:
            full_url_query = url_query + '?' + '&'.join([f'{k}={v}' for k, v in arr_query.items()])
            
        async with aiohttp.ClientSession() as session:
            if method_query == 'PUT':
                async with session.put(full_url_query, headers=self.header , ssl=False) as response:
                    result_query = await response.text()
            elif method_query == 'POST':
                async with session.post(full_url_query, data=arr_query, headers=self.header , ssl=False) as response:
                    result_query = await response.text()
            elif method_query == 'DELETE':
                async with session.delete(full_url_query, headers=self.header , ssl=False) as response:
                    result_query = await response.text()
            else:  # GET
                async with session.get(full_url_query, headers=self.header , ssl=False) as response:
                    result_query = await response.text()

        return json.loads(result_query) if result_query else []

    async def disk_info_public_res(self, public_key, path='/'):
        arr_params = {'public_key': public_key,
                      'path': path}
        url_query = 'https://cloud-api.yandex.net/v1/disk/public/resources'
        return await self.send_query_yad(url_query, arr_params)

