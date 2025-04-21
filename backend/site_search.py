import os
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
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
import redis.asyncio as aioredis
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
        self.db_configs = {
            "cms": {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "dbname": os.getenv("DB_NAME", "cms"),
                "user": os.getenv("DB_USER", "postgres"),
                "password": os.getenv("DB_PASS"),
            },
            "lists": {
                "host": os.getenv("LISTS_DB_HOST", os.getenv("DB_HOST", "localhost")),
                "port": int(os.getenv("LISTS_DB_PORT", os.getenv("DB_PORT", "5432"))),
                "dbname": os.getenv("LISTS_DB_NAME", "lists"),
                "user": os.getenv("LISTS_DB_USER", os.getenv("DB_USER", "postgres")),
                "password": os.getenv("LISTS_DB_PASS", os.getenv("DB_PASS")),
            },
            "filestorage": {
                "host": os.getenv("STORAGE_DB_HOST", os.getenv("DB_HOST", "localhost")),
                "port": int(os.getenv("STORAGE_DB_PORT", os.getenv("DB_PORT", "5432"))),
                "dbname": os.getenv("STORAGE_DB_NAME", "filestorage"),
                "user": os.getenv("STORAGE_DB_USER", os.getenv("DB_USER", "postgres")),
                "password": os.getenv("STORAGE_DB_PASS", os.getenv("DB_PASS")),
            },
        }

        self.redis_config = {
            "host": os.getenv("REDIS_HOST", "localhost"),
            "port": int(os.getenv("REDIS_PORT", "6379")),
            "db": int(os.getenv("REDIS_DB", "0"))
        }

        self.cache_dir = "file_cache"
        self.cache_ttl = cache_ttl
        self.redis = aioredis.Redis(**self.redis_config)
        self.executor = ThreadPoolExecutor(max_workers=int(os.getenv("THREAD_WORKERS", "8")))
        self.process_executor = ProcessPoolExecutor(max_workers=int(os.getenv("PROCESS_WORKERS", "4")))
        os.makedirs(self.cache_dir, exist_ok=True)

    async def _get_async_db_connection(self, db_type: str = "cms"):
        config = self.db_configs[db_type]
        return await psycopg.AsyncConnection.connect(
            host=config["host"],
            port=config["port"],
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"],
            row_factory=dict_row
        )

    async def _get_site_id_by_name(self, site_name: str) -> Optional[str]:
        async with await self._get_async_db_connection("cms") as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id FROM sites_site WHERE name = %s AND status = 'published'",
                    (site_name,)
                )
                result = await cur.fetchone()
                return result["id"] if result else None

    async def get_site_content(self, site_name: str) -> AsyncGenerator[Dict, None]:
        site_id = await self._get_site_id_by_name(site_name)
        if not site_id:
            raise ValueError(f"Site {site_name} not found or not published")

        pages_gen = self._stream_pages_content(site_id)
        files_gen = self._stream_files_content(site_id)
        lists_gen = self._stream_lists_content(site_id)

        async for item in self._priority_merge_generators(pages_gen, files_gen, lists_gen):
            yield item

    async def _priority_merge_generators(self, *generators) -> AsyncGenerator:
        tasks = {i: asyncio.create_task(gen.__anext__()) for i, gen in enumerate(generators)}
        while tasks:
            done, _ = await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)
            for idx, task in list(tasks.items()):
                if task in done:
                    try:
                        yield task.result()
                        tasks[idx] = asyncio.create_task(generators[idx].__anext__())
                    except StopAsyncIteration:
                        del tasks[idx]

    async def _stream_pages_content(self, site_id: str) -> AsyncGenerator[Dict, None]:
        query = """
            SELECT pp.id, pp.name, pp.body, pp.slug, pp.created_at, pp.updated_at, u1.email as created_by
            FROM pages_page pp
            JOIN sites_serviceobject so ON so.external_id = pp.id::TEXT
            LEFT JOIN users_user u1 ON pp.created_by_id = u1.keycloak_id
            WHERE so.site_id = %s AND pp.status = 'published'
            ORDER BY pp.updated_at DESC
        """
        async with await self._get_async_db_connection("cms") as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (site_id,))
                rows = await cur.fetchall()
                for row in rows:
                    result = await self._process_page(row)
                    if result:
                        yield result

    async def _process_page(self, row) -> Optional[Dict]:
        return await asyncio.get_event_loop().run_in_executor(
            self.executor, lambda: self._process_page_sync(row)
        )

    def _process_page_sync(self, row) -> Optional[Dict]:
        try:
            content = self._extract_text_from_html(row["body"])
            if not content:
                return None
            return {
                "content": content,
                "metadata": {
                    "id": row["id"],
                    "title": row["name"],
                    "slug": row["slug"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "created_by": row["created_by"],
                    "type": ContentType.HTML.value,
                },
            }
        except Exception as e:
            logger.error(f"Error processing page {row.get('id')}: {e}")
            return None

    def _extract_text_from_html(self, html: str) -> Optional[str]:
        try:
            soup = BeautifulSoup(html, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()
            return soup.get_text(separator="\n", strip=True)
        except Exception as e:
            logger.error(f"HTML parsing error: {e}")
            return None

    async def _stream_files_content(self, site_id: str) -> AsyncGenerator[Dict, None]:
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
            async with conn.cursor() as cur:
                await cur.execute(query, (root_folder_id,))
                rows = await cur.fetchall()
                for row in rows:
                    try:
                        content = await self._process_file(row["file_link"])
                        if content:
                            yield {
                                "content": content,
                                "metadata": {
                                    "id": row["id"],
                                    "name": row["name"],
                                    "type": ContentType.FILE.value,
                                    "url": row["file_link"],
                                },
                            }
                    except Exception as e:
                        logger.error(f"Error processing file {row.get('id')}: {e}")

    async def _get_root_folder_id(self, site_id: str) -> Optional[str]:
        async with await self._get_async_db_connection("cms") as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT root_folder_id FROM sites_site WHERE id = %s", (site_id,))
                result = await cur.fetchone()
                return result["root_folder_id"] if result else None

    async def _process_file(self, file_url: str) -> Optional[str]:
        cache_key = f"file:{hashlib.md5(file_url.encode()).hexdigest()}"
        cached = await self.redis.get(cache_key)
        if cached:
            return orjson.loads(cached)

        local_cache_file = os.path.join(self.cache_dir, f"{file_url.replace('/', '_')}.json")
        if os.path.exists(local_cache_file):
            async with aiofiles.open(local_cache_file, 'r') as f:
                content = await f.read()
                await self.redis.setex(cache_key, self.cache_ttl, content)
                return orjson.loads(content)

        try:
            base_url = "https://hackaton.hb.ru-msk.vkcloud-storage.ru/media/"
            async with aiohttp.ClientSession() as session:
                async with session.get(base_url + file_url, timeout=30) as response:
                    response.raise_for_status()
                    file_content = await response.read()

            content = await asyncio.get_event_loop().run_in_executor(
                self.process_executor, self._parse_file_content, BytesIO(file_content)
            )

            await self.redis.setex(cache_key, self.cache_ttl, orjson.dumps(content))
            async with aiofiles.open(local_cache_file, "wb") as f:
                await f.write(orjson.dumps(content))

            return content
        except Exception as e:
            logger.error(f"File processing error for {file_url}: {e}")
            return None

    def _parse_file_content(self, file_obj: BytesIO) -> str:
        try:
            elements = partition(file=file_obj)
            return "\n".join(str(el) for el in elements)
        except Exception as e:
            logger.error(f"Content parsing error: {e}")
            return ""

    async def _stream_lists_content(self, site_id: str) -> AsyncGenerator[Dict, None]:
        query = """
            SELECT ll.id, ll.name, jsonb_agg(lr.data) as items
            FROM lists_list ll
            JOIN sites_serviceobject so ON so.external_id = ll.id::TEXT
            LEFT JOIN lists_list_row lr ON ll.id = lr.list_id
            WHERE so.site_id = %s
            GROUP BY ll.id
        """
        async with await self._get_async_db_connection("lists") as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (site_id,))
                rows = await cur.fetchall()
                for row in rows:
                    try:
                        content = "\n".join(f"• {item}" for item in row["items"] if item)
                        if content:
                            yield {
                                "content": content,
                                "metadata": {
                                    "id": row["id"],
                                    "name": row["name"],
                                    "type": ContentType.LIST.value,
                                    "item_count": len(row["items"]),
                                },
                            }
                    except Exception as e:
                        logger.error(f"Error processing list {row.get('id')}: {e}")

    async def close(self):
        self.executor.shutdown()
        self.process_executor.shutdown()
        await self.redis.close()
