import asyncio
import aiohttp
import logging
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')

INDEX_URL = 'https://spa5.scrape.center/api/book/?limit=18&offset={offset}'
DETAIL_URL = 'https://spa5.scrape.center/api/book/{id}'
PAGE_SIZE = 18
PAGE_NUMBER = 100

session = aiohttp.ClientSession()

MONGO_CONNECTION_STRING = 'mongodb://localhost:27017'
MONGO_DB_NAME = 'books'
MONGO_COLLECTION_NAME = 'books'

client = AsyncIOMotorClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DB_NAME]
collection = db[MONGO_CONNECTION_STRING]


async def scrape_api(url):
    logging.info('scraping %s', url)
    async with session.get(url) as response:
        return await response.json()


async def scrape_index(page):
    url = INDEX_URL.format(offset=PAGE_SIZE * (page - 1))
    return await scrape_api(url)


async def scrape_detail(id):
    url = DETAIL_URL.format(id=id)
    return await scrape_api(url)


async def save_data(data):
    logging.info('saving data %s', data)
    return await collection.update_one({
        'id': data.get('id')
    }, {
        '$set': data
    }, upsert=True)


async def main():
    for page in range(1, PAGE_NUMBER + 1):
        index_data = await scrape_index(page)
        logging.info('index data %s', index_data)
        for item in index_data.get('results'):
            detail_data = await scrape_detail(item.get('id'))
            await save_data(detail_data)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
