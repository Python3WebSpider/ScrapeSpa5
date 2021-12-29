import asyncio
import json
import time

import aiohttp
import logging

from aiohttp import ContentTypeError
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')

INDEX_URL = 'https://spa5.scrape.center/api/book/?limit=18&offset={offset}'
DETAIL_URL = 'https://spa5.scrape.center/api/book/{id}'
PAGE_SIZE = 18
PAGE_NUMBER = 1
CONCURRENCY = 5

MONGO_CONNECTION_STRING = 'mongodb://localhost:27017'
MONGO_DB_NAME = 'books'
MONGO_COLLECTION_NAME = 'books'

client = AsyncIOMotorClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DB_NAME]
collection = db[MONGO_CONNECTION_STRING]

loop = asyncio.get_event_loop()


class Spider(object):
    
    def __init__(self):
        self.semaphore = asyncio.Semaphore(CONCURRENCY)
    
    async def scrape_api(self, url):
        async with self.semaphore:
            try:
                logging.info('scraping %s', url)
                async with self.session.get(url) as response:
                    await asyncio.sleep(1)
                    return await response.json()
            except ContentTypeError as e:
                logging.error('error occurred while scraping %s', url, exc_info=True)
    
    async def scrape_index(self, page):
        url = INDEX_URL.format(offset=PAGE_SIZE * (page - 1))
        return await self.scrape_api(url)
    
    async def scrape_detail(self, id):
        url = DETAIL_URL.format(id=id)
        data = await self.scrape_api(url)
        await self.save_data(data)
    
    async def save_data(self, data):
        logging.info('saving data %s', data)
        if data:
            return await collection.update_one({
                'id': data.get('id')
            }, {
                '$set': data
            }, upsert=True)
    
    async def main(self):
        self.session = aiohttp.ClientSession()
        # index tasks
        scrape_index_tasks = [asyncio.ensure_future(self.scrape_index(page)) for page in range(1, PAGE_NUMBER + 1)]
        results = await asyncio.gather(*scrape_index_tasks)
        # detail tasks
        print('results', results)
        ids = []
        for index_data in results:
            if not index_data: continue
            for item in index_data.get('results'):
                ids.append(item.get('id'))
        scrape_detail_tasks = [asyncio.ensure_future(self.scrape_detail(id)) for id in ids]
        await asyncio.wait(scrape_detail_tasks)
        await self.session.close()


if __name__ == '__main__':
    spider = Spider()
    loop.run_until_complete(spider.main())
