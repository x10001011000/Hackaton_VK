import asyncio
import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
import aiohttp
import aiofiles
from io import BytesIO
from unstructured.partition.auto import partition
import os
import json
from datetime import datetime
from typing import List, Dict, Optional, AsyncGenerator
from enum import Enum
import hashlib
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import redis
import orjson

class ContentType(Enum):
    HTML = "html"
    FILE = "file"
    LIST = "list"

class SiteSearchEngine:
    def __init__(self, db_config: Dict[str, str], cache_ttl: int = 3600):
        self.db_config = db_config
        self.cache_dir = "file_cache"
        self.cache_ttl = cache_ttl
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.process_executor = ProcessPoolExecutor(max_workers=4)
        os.makedirs(self.cache_dir, exist_ok=True)
        self._sites_cache = {}
        self._last_cache_update = datetime.min

    async def initialize(self):
        await self._preload_sites_cache()

    async def _preload_sites_cache(self):
        if (datetime.now() - self._last_cache_update).total_seconds() > 300:
            self._sites_cache = {site['name']: site for site in await self._get_all_sites()}
            self._last_cache_update = datetime.now()

    async def get_available_sites(self) -> List[str]:
        await self._preload_sites_cache()
        return list(self._sites_cache.keys())

    async def get_site_content(self, site_name: str) -> AsyncGenerator[Dict, None]:
        site_info = self._sites_cache.get(site_name)
        if not site_info:
            raise ValueError(f"Site {site_name} not found")

        tasks = [
            self._stream_pages_content(site_info['id']),
            self._stream_files_content(site_info['filestorage_root_folder_id']),
            self._stream_lists_content(site_info['id'])
        ]
        async for chunk in self._merge_async_generators(*tasks):
            yield chunk

    async def close(self):
        self.executor.shutdown()
        self.process_executor.shutdown()
        await self.redis.close()
