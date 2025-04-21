import os
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
import aiohttp
import aiofiles
from io import BytesIO
from unstructured.partition.auto import partition
import json
from datetime import datetime
from typing import List, Dict, Optional, AsyncGenerator
from enum import Enum
import hashlib
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import redis
import orjson
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class ContentType(Enum):
    HTML = "html"
    FILE = "file"
    LIST = "list"

class SiteSearchEngine:
    def __init__(self, cache_ttl: int = 3600):
        # Конфигурации для разных БД
        self.db_configs = {
            "cms": {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "dbname": os.getenv("DB_NAME", "cms"),
                "user": os.getenv("DB_USER", "postgres"),
                "password": os.getenv("DB_PASS"),
                "application_name": "SiteSearchEngine_CMS"
            },
            "lists": {
                "host": os.getenv("LISTS_DB_HOST", os.getenv("DB_HOST", "localhost")),
                "port": int(os.getenv("LISTS_DB_PORT", os.getenv("DB_PORT", "5432"))),
                "dbname": os.getenv("LISTS_DB_NAME", "lists"),
                "user": os.getenv("LISTS_DB_USER", os.getenv("DB_USER", "postgres")),
                "password": os.getenv("LISTS_DB_PASS", os.getenv("DB_PASS")),
                "application_name": "SiteSearchEngine_Lists"
            },
            "filestorage": {
                "host": os.getenv("STORAGE_DB_HOST", os.getenv("DB_HOST", "localhost")),
                "port": int(os.getenv("STORAGE_DB_PORT", os.getenv("DB_PORT", "5432"))),
                "dbname": os.getenv("STORAGE_DB_NAME", "filestorage"),
                "user": os.getenv("STORAGE_DB_USER", os.getenv("DB_USER", "postgres")),
                "password": os.getenv("STORAGE_DB_PASS", os.getenv("DB_PASS")),
                "application_name": "SiteSearchEngine_Storage"
            }
        }
        
        # Настройки Redis
        self.redis_config = {
            "host": os.getenv("REDIS_HOST", "localhost"),
            "port": int(os.getenv("REDIS_PORT", "6379")),
            "db": int(os.getenv("REDIS_DB", "0"))
        }

        self.cache_dir = "file_cache"
        self.cache_ttl = cache_ttl
        self.redis = redis.Redis(**self.redis_config)
        self.executor = ThreadPoolExecutor(max_workers=int(os.getenv("THREAD_WORKERS", "8")))
        self.process_executor = ProcessPoolExecutor(max_workers=int(os.getenv("PROCESS_WORKERS", "4")))
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self._sites_cache = {}
        self._last_cache_update = datetime.min

    async def _get_async_db_connection(self, db_type: str = "cms"):
        """Асинхронное подключение к PostgreSQL для указанного типа БД"""
        config = self.db_configs.get(db_type, self.db_configs["cms"])
        return await psycopg2.connect(
            host=config["host"],
            port=config["port"],
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"],
            async_=True
        )

    async def _get_site_id_by_name(self, site_name: str) -> Optional[str]:
        """Получение ID сайта по названию из основной БД"""
        async with await self._get_async_db_connection("cms") as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id FROM sites_site WHERE name = %s AND status = 'published'",
                    (site_name,)
                )
                result = await cur.fetchone()
                return result[0] if result else None

    async def get_site_content(self, site_name: str) -> AsyncGenerator[Dict, None]:
        """Основной метод получения контента"""
        site_id = await self._get_site_id_by_name(site_name)
        if not site_id:
            raise ValueError(f"Site {site_name} not found or not published")

        # Создаем задачи для каждого типа контента
        pages_gen = self._stream_pages_content(site_id)
        files_gen = self._stream_files_content(site_id)
        lists_gen = self._stream_lists_content(site_id)

        # Объединяем генераторы с приоритетом страниц, затем файлов, затем списков
        async for item in self._priority_merge_generators(pages_gen, files_gen, lists_gen):
            yield item

    async def _priority_merge_generators(self, *generators) -> AsyncGenerator:
        """Объединение генераторов с приоритетом в порядке их перечисления"""
        pending = {i: None for i in range(len(generators))}
        
        # Инициализация всех генераторов
        for i, gen in enumerate(generators):
            try:
                pending[i] = asyncio.create_task(gen.__anext__())
            except StopAsyncIteration:
                pending.pop(i, None)

        while pending:
            # Ожидаем завершения любой задачи
            done, _ = await asyncio.wait(
                pending.values(),
                return_when=asyncio.FIRST_COMPLETED
            )

            for i, task in list(pending.items()):
                if task in done:
                    try:
                        yield task.result()
                        # Запускаем следующую итерацию этого генератора
                        new_task = asyncio.create_task(generators[i].__anext__())
                        pending[i] = new_task
                    except StopAsyncIteration:
                        pending.pop(i, None)

    async def _stream_pages_content(self, site_id: str) -> AsyncGenerator[Dict, None]:
        """Потоковая загрузка страниц из CMS"""
        query = """
            SELECT pp.id, pp.name, pp.body, pp.slug, pp.created_at, 
                   pp.updated_at, u1.email as created_by
            FROM pages_page pp
            JOIN sites_serviceobject so ON so.external_id = pp.id::TEXT
            LEFT JOIN users_user u1 ON pp.created_by_id = u1.keycloak_id
            WHERE so.site_id = %s AND pp.status = 'published'
            ORDER BY pp.updated_at DESC
        """

        async with await self._get_async_db_connection("cms") as conn:
            async with conn.cursor(name='pages_cursor', 
                                 cursor_factory=psycopg2.extras.DictCursor) as cur:
                await cur.execute(query, (site_id,))
                
                batch = []
                async for row in cur:
                    batch.append(row)
                    if len(batch) >= 50:
                        processed = await self._process_pages_batch(batch)
                        for item in processed:
                            if item:  # Пропускаем None
                                yield item
                        batch = []
                
                if batch:
                    processed = await self._process_pages_batch(batch)
                    for item in processed:
                        if item:
                            yield item

    async def _process_pages_batch(self, batch: List) -> List[Dict]:
        """Параллельная обработка пакета страниц"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            lambda: [self._process_page(row) for row in batch]
        )

    def _process_page(self, row) -> Optional[Dict]:
        """Синхронная обработка одной страницы"""
        try:
            content = self._extract_text_from_html(row['body'])
            if not content:
                return None

            return {
                "content": content,
                "metadata": {
                    "id": row['id'],
                    "title": row['name'],
                    "slug": row['slug'],
                    "created_at": row['created_at'],
                    "updated_at": row['updated_at'],
                    "created_by": row['created_by'],
                    "type": ContentType.HTML.value
                }
            }
        except Exception as e:
            logger.error(f"Error processing page {row.get('id')}: {e}")
            return None

    def _extract_text_from_html(self, html: str) -> Optional[str]:
        """Извлечение текста из HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Удаляем скрипты и стили
            for script in soup(["script", "style"]):
                script.decompose()
            return soup.get_text(separator='\n', strip=True)
        except Exception as e:
            logger.error(f"HTML parsing error: {e}")
            return None

    async def _stream_files_content(self, site_id: str) -> AsyncGenerator[Dict, None]:
        """Потоковая загрузка файлов из хранилища"""
        # Сначала получаем root_folder_id для сайта
        root_folder_id = await self._get_root_folder_id(site_id)
        if not root_folder_id:
            return

        query = """
            WITH RECURSIVE folder_tree AS (
                SELECT id FROM storage_storageobject WHERE id = %s
                UNION
                SELECT so.id FROM storage_storageobject so
                JOIN folder_tree ft ON so.parent_id = ft.id
            )
            SELECT so.id, so.name, sv.link as file_link
            FROM folder_tree ft
            JOIN storage_storageobject so ON ft.id = so.id
            JOIN storage_version sv ON so.id = sv.storage_object_id
            WHERE so.type = 1
            ORDER BY sv.created_at DESC
        """

        async with await self._get_async_db_connection("filestorage") as conn:
            async with conn.cursor(name='files_cursor',
                                 cursor_factory=psycopg2.extras.DictCursor) as cur:
                await cur.execute(query, (root_folder_id,))
                
                async for row in cur:
                    try:
                        content = await self._process_file(row['file_link'])
                        if content:
                            yield {
                                "content": content,
                                "metadata": {
                                    "id": row['id'],
                                    "name": row['name'],
                                    "type": ContentType.FILE.value,
                                    "url": row['file_link']
                                }
                            }
                    except Exception as e:
                        logger.error(f"Error processing file {row.get('id')}: {e}")
                        continue

    async def _get_root_folder_id(self, site_id: str) -> Optional[str]:
        """Получение root_folder_id для сайта из основной БД"""
        async with await self._get_async_db_connection("cms") as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT root_folder_id FROM sites_site WHERE id = %s",
                    (site_id,)
                )
                result = await cur.fetchone()
                return result[0] if result else None

    async def _process_file(self, file_url: str) -> Optional[str]:
        """Асинхронная обработка файла с кешированием"""
        cache_key = f"file:{hashlib.md5(file_url.encode()).hexdigest()}"
        
        # Проверка кеша Redis
        cached = self.redis.get(cache_key)
        if cached:
            return orjson.loads(cached)

        # Проверка локального кеша
        local_cache_file = os.path.join(self.cache_dir, f"{file_url.replace('/', '_')}.json")
        if os.path.exists(local_cache_file):
            async with aiofiles.open(local_cache_file, 'r') as f:
                content = await f.read()
                self.redis.setex(cache_key, self.cache_ttl, content)
                return orjson.loads(content)

        # Загрузка и обработка файла
        try:
            base_url = "https://hackaton.hb.ru-msk.vkcloud-storage.ru/media/"
            async with aiohttp.ClientSession() as session:
                async with session.get(base_url + file_url, timeout=30) as response:
                    response.raise_for_status()
                    file_content = await response.read()

            # Обработка в отдельном процессе
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(
                self.process_executor,
                self._parse_file_content,
                BytesIO(file_content)
            )

            # Сохранение в кеши
            self.redis.setex(cache_key, self.cache_ttl, orjson.dumps(content))
            async with aiofiles.open(local_cache_file, 'wb') as f:
                await f.write(orjson.dumps(content))

            return content
        except Exception as e:
            logger.error(f"File processing error for {file_url}: {e}")
            return None

    def _parse_file_content(self, file_obj: BytesIO) -> str:
        """Синхронная обработка содержимого файла"""
        try:
            elements = partition(file=file_obj)
            return "\n".join([str(el) for el in elements])
        except Exception as e:
            logger.error(f"Content parsing error: {e}")
            return ""

    async def _stream_lists_content(self, site_id: str) -> AsyncGenerator[Dict, None]:
        """Потоковая загрузка списков из БД списков"""
        query = """
            SELECT ll.id, ll.name, jsonb_agg(lr.data) as items
            FROM lists_list ll
            JOIN sites_serviceobject so ON so.external_id = ll.id::TEXT
            LEFT JOIN lists_list_row lr ON ll.id = lr.list_id
            WHERE so.site_id = %s
            GROUP BY ll.id
        """

        async with await self._get_async_db_connection("lists") as conn:
            async with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                await cur.execute(query, (site_id,))
                
                async for row in cur:
                    try:
                        content = "\n".join(f"• {item}" for item in row['items'] if item)
                        if content:
                            yield {
                                "content": content,
                                "metadata": {
                                    "id": row['id'],
                                    "name": row['name'],
                                    "type": ContentType.LIST.value,
                                    "item_count": len(row['items'])
                                }
                            }
                    except Exception as e:
                        logger.error(f"Error processing list {row.get('id')}: {e}")
                        continue

    async def close(self):
        """Корректное завершение работы"""
        self.executor.shutdown()
        self.process_executor.shutdown()
        self.redis.close()
