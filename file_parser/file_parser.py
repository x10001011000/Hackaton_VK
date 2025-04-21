import os
import json
import requests
import psycopg2
from io import BytesIO
from tqdm import tqdm
from psycopg2.extras import RealDictCursor
from unstructured.partition.auto import partition

# Конфигурация
CACHE_DIR = "file_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def get_db_connection(db_config):
    """Устанавливает соединение с PostgreSQL"""
    return psycopg2.connect(
        dbname=db_config.get("dbname", "postgres"),
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )

def get_all_files_by_site_name(site_name, db_cms_credentials, db_filestorage_credentials):
    """Получает все файлы сайта из базы данных"""
    try:
        # Подключение к CMS
        with get_db_connection(db_cms_credentials) as conn_cms, \
             conn_cms.cursor(cursor_factory=RealDictCursor) as cursor_cms:
            
            cursor_cms.execute("""
                SELECT id, name, filestorage_root_folder_id 
                FROM public.sites_site 
                WHERE name = %s 
                LIMIT 1
            """, (site_name,))
            site = cursor_cms.fetchone()

            if not site:
                print(f"Сайт '{site_name}' не найден!")
                return []
            
            root_folder_id = site["filestorage_root_folder_id"]
            if not root_folder_id:
                print("У сайта нет корневой папки!")
                return []

        # Подключение к filestorage
        with get_db_connection(db_filestorage_credentials) as conn_fs, \
             conn_fs.cursor(cursor_factory=RealDictCursor) as cursor_fs:
            
            cursor_fs.execute("""
                WITH RECURSIVE folder_tree AS (
                    SELECT id, name, parent_id
                    FROM public.storage_storageobject
                    WHERE id = %s AND type = 0
                    UNION ALL
                    SELECT child.id, child.name, child.parent_id
                    FROM public.storage_storageobject child
                    JOIN folder_tree parent ON child.parent_id = parent.id
                ),
                latest_versions AS (
                    SELECT 
                        storage_object_id,
                        link,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY storage_object_id 
                            ORDER BY created_at DESC
                        ) as rn
                    FROM public.storage_version
                )
                SELECT
                    f.id,
                    f.name,
                    f.size,
                    lv.link as file_url,
                    ARRAY(
                        SELECT c.name 
                        FROM public.storage_category c
                        JOIN public.storage_storageobject_categories sc 
                            ON c.id = sc.category_id
                        WHERE sc.storageobject_id = f.id
                    ) as categories
                FROM folder_tree ft
                JOIN public.storage_storageobject f ON ft.id = f.id
                LEFT JOIN latest_versions lv ON f.id = lv.storage_object_id AND lv.rn = 1
                WHERE f.type = 1
                ORDER BY f.name
            """, (root_folder_id,))

            return cursor_fs.fetchall()

    except Exception as e:
        print(f"Ошибка при работе с БД: {e}")
        return []

def get_file_content(link):
    """Загружает и парсит файл по ссылке"""
    base_url = "https://hackaton.hb.ru-msk.vkcloud-storage.ru/media/"
    try:
        response = requests.get(base_url + link, timeout=10)
        response.raise_for_status()
        file = BytesIO(response.content)
        elements = partition(file=file)
        return "\n".join([str(el) for el in elements])
    except Exception as e:
        print(f"Ошибка при обработке файла {link}: {e}")
        return None

def get_cached_text(link):
    """Проверяет наличие текста в кеше"""
    cache_file = os.path.join(CACHE_DIR, f"{link.replace('/', '_')}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                return json.load(f).get("text")
        except json.JSONDecodeError:
            return None
    return None

def save_to_cache(link, text):
    """Сохраняет текст в кеш"""
    cache_file = os.path.join(CACHE_DIR, f"{link.replace('/', '_')}.json")
    with open(cache_file, "w") as f:
        json.dump({"link": link, "text": text}, f)

def process_files(links):
    """Обрабатывает список файлов с кешированием"""
    all_texts = []
    for link in tqdm(links, desc="Обработка файлов"):
        if not link:
            continue
            
        cached_text = get_cached_text(link)
        if cached_text:
            all_texts.append(f"=== Файл: {link} ===\n{cached_text}\n")
            continue
        
        content = get_file_content(link)
        if content:
            save_to_cache(link, content)
            all_texts.append(f"=== Файл: {link} ===\n{content}\n")
    
    return "\n".join(all_texts)

def main():
    """Основная функция"""
    # Конфигурация БД
    DB_CONFIG = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "123098"  # Замените на реальный пароль
    }

    site_name = input("Введите название сайта: ")
    
    # Получаем файлы
    files = get_all_files_by_site_name(site_name, DB_CONFIG, DB_CONFIG)
    if not files:
        return
        
    print(f"\nНайдено файлов: {len(files)}")
    for file in files:
        print(f"  {file['name']} (URL: {file['file_url']})")
    
    # Обрабатываем файлы
    file_urls = [file['file_url'] for file in files if file.get('file_url')]
    combined_text = process_files(file_urls)
    
    print(f"\nОбщий объём текста: {len(combined_text)} символов")
    return combined_text

if __name__ == "__main__":
    main()