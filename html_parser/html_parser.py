import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup

def get_site_pages_text(site_name):
    hostname = "localhost"
    database = "cms"
    username = "postgres"
    password = "123456"
    port_id = 5432

    try:
        with psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=password,
            port=port_id
        ) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                query = """
                    SELECT pp.body
                    FROM sites_site ss
                    JOIN sites_serviceobject so ON ss.id = so.site_id
                    JOIN pages_page pp ON pp.id::TEXT = so.external_id
                    WHERE ss.name = %s
                      AND pp.status = 'published';
                """
                cur.execute(query, (site_name,))
                results = cur.fetchall()

                all_texts = []
                for row in results:
                    body_content = row['body']

                    if not body_content:
                        continue

                    if isinstance(body_content, str):
                        soup = BeautifulSoup(body_content, "html.parser")
                        text = soup.get_text(separator=" ", strip=True)
                        all_texts.append(text)

                    elif isinstance(body_content, dict):
                        for key, value in body_content.items():
                            if isinstance(value, str):
                                soup = BeautifulSoup(value, "html.parser")
                                text = soup.get_text(separator=" ", strip=True)
                                if text:
                                    all_texts.append(text)

                full_text = "\n\n".join(all_texts)
                return full_text

    except Exception as e:
        print(f"Ошибка при обработке: {e}")
        return None
