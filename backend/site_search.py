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

# Загрузка переменных окружения
load_dotenv()

class ContentType(Enum):
    HTML = "html"
    FILE = "file"
    LIST = "list"

class SiteSearchEngine:
    def __init__(self, cache_ttl: int = 3600):
        # Получение настроек БД из .env
        self.db_config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "5432")),
            "dbname": os.getenv("DB_NAME", "cms"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASS"),
            "application_name": "SiteSearchEngine"  # Для мониторинга в pgAdmin
        }
        
        # Настройки Redis
        self.redis_config = {
            "host": os.getenv("REDIS_HOST", "localhost"),
            "port": int(os.getenv("REDIS_PORT", "6379")),
            "db": int(os.getenv("REDIS_DB", "0"))
        }

        # Инициализация компонентов
        self.cache_dir = "file_cache"
        self.cache_ttl = cache_ttl
        self.redis = redis.Redis(**self.redis_config)
        self.executor = ThreadPoolExecutor(max_workers=int(os.getenv("THREAD_WORKERS", "8")))
        self.process_executor = ProcessPoolExecutor(max_workers=int(os.getenv("PROCESS_WORKERS", "4")))
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Кеш данных о сайтах
        self._sites_cache = {}
        self._last_cache_update = datetime.min

    async def _get_async_db_connection(self):
        """Асинхронное подключение к PostgreSQL"""
        return await psycopg2.connect(
            host=self.db_config["host"],
            port=self.db_config["port"],
            dbname=self.db_config["dbname"],
            user=self.db_config["user"],
            password=self.db_config["password"],
            async_=True
        )

    async def _get_site_id_by_name(self, site_name: str) -> Optional[str]:
        """Получение ID сайта по названию"""
        async with await self._get_async_db_connection() as conn:
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

        # Параллельная загрузка разных типов контента
        tasks = [
            self._stream_pages_content(site_id),
            self._stream_files_content(site_id),
            self._stream_lists_content(site_id)
        ]

        async for chunk in self._merge_async_generators(*tasks):
            yield chunk

    async def close(self):
        """Корректное завершение работы"""
        self.executor.shutdown()
        self.process_executor.shutdown()
        await self.redis.close()
