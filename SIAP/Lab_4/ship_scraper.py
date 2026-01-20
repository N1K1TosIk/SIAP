import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
import re


async def extract_ship_from_details_page(session, url, headers):
    """Извлекает информацию о судне со страницы деталей"""
    try:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                return None
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
        
        ship_data = {}
        
        # Извлекаем название из h1
        h1 = soup.find('h1')
        if h1:
            ship_data['Name'] = h1.get_text(strip=True)
        
        # Извлекаем IMO и тип из h2
        h2 = soup.find('h2')
        if h2:
            h2_text = h2.get_text()
            imo_match = re.search(r'IMO\s*(\d+)', h2_text, re.IGNORECASE)
            if imo_match:
                ship_data['IMO'] = imo_match.group(1)
            
            parts = h2_text.split(',')
            if parts:
                type_candidate = parts[0].strip()
                if type_candidate and 'IMO' not in type_candidate.upper():
                    ship_data['Type'] = type_candidate
        
        # Ищем MMSI, IMO и тип в таблицах
        for table in soup.find_all('table'):
            for row in table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    
                    if 'mmsi' in label and 'MMSI' not in ship_data:
                        mmsi_match = re.search(r'\d+', value)
                        if mmsi_match:
                            ship_data['MMSI'] = mmsi_match.group(0)
                    
                    if 'imo' in label and 'IMO' not in ship_data:
                        imo_match = re.search(r'\d+', value)
                        if imo_match:
                            ship_data['IMO'] = imo_match.group(0)
                    
                    if ('type' in label or 'тип' in label) and 'Type' not in ship_data:
                        ship_data['Type'] = value
        
        # Ищем MMSI в тексте страницы
        if 'MMSI' not in ship_data:
            page_text = soup.get_text()
            mmsi_match = re.search(r'MMSI[:\s]*(\d+)', page_text, re.IGNORECASE)
            if mmsi_match:
                ship_data['MMSI'] = mmsi_match.group(1)
        
        if ship_data.get('Name') or ship_data.get('IMO'):
            return {
                'Name': ship_data.get('Name', 'N/A'),
                'IMO': ship_data.get('IMO', 'N/A'),
                'MMSI': ship_data.get('MMSI', 'N/A'),
                'Type': ship_data.get('Type', 'N/A')
            }
        return None
        
    except Exception as e:
        print(f"Ошибка при извлечении данных {url}: {e}")
        return None


async def extract_ship_info(session, url, headers):
    """Извлекает информацию о судах со страницы"""
    try:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                print(f"HTTP {response.status} для {url}")
                return None
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
        
        # Если это страница деталей
        if '/vessels/details/' in url:
            ship = await extract_ship_from_details_page(session, url, headers)
            return [ship] if ship else []
        
        # Страница поиска - считаем количество судов
        detail_links = soup.find_all('a', href=re.compile(r'/vessels/details/\d+'))
        ship_count = len(detail_links)
        
        if ship_count == 0:
            return []
        
        # Больше одного судна - пропускаем
        if ship_count > 1:
            return [None] * ship_count
        
        # Ровно одно судно - получаем данные
        if ship_count == 1 and detail_links:
            details_url = urljoin(url, detail_links[0]['href'])
            ship = await extract_ship_from_details_page(session, details_url, headers)
            return [ship] if ship else []
        
        return []
        
    except aiohttp.ClientError as e:
        print(f"Ошибка сети для {url}: {type(e).__name__}: {e}")
        return None
    except Exception as e:
        print(f"Ошибка при обработке {url}: {type(e).__name__}: {e}")
        return None


async def process_single_link(session, semaphore, url, headers):
    """Обрабатывает одну ссылку с ограничением параллелизма"""
    if not url or not url.startswith('http'):
        return None
    
    async with semaphore:
        try:
            ships = await extract_ship_info(session, url, headers)
            
            if ships is None:
                return None
            
            if len(ships) == 0:
                print(f"Судов не найдено: {url}")
                return None
            
            if len(ships) > 1:
                print(f"Найдено {len(ships)} судов: {url}")
                return None
            
            return ships[0]
        except Exception as e:
            print(f"Ошибка в process_single_link для {url}: {type(e).__name__}: {e}")
            return None


async def process_links_async(input_file='Links.xlsx', output_file='result.xlsx'):
    """Асинхронно обрабатывает ссылки из файла"""
    try:
        df_links = pd.read_excel(input_file)
        
        link_column = None
        for col in df_links.columns:
            if 'link' in str(col).lower() or 'url' in str(col).lower():
                link_column = col
                break
        
        if link_column is None:
            link_column = df_links.columns[0]
        
    except Exception as e:
        print(f"Ошибка при чтении файла {input_file}: {e}")
        return
    
    urls = [str(row[link_column]).strip() for _, row in df_links.iterrows()]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
    }
    
    # Ограничиваем параллелизм для избежания блокировки
    semaphore = asyncio.Semaphore(5)  # Уменьшено для избежания блокировки
    connector = aiohttp.TCPConnector(limit=20, limit_per_host=3, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=90, connect=30)
    
    results = []
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [process_single_link(session, semaphore, url, headers) for url in urls]
        
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                if result:
                    results.append(result)
            except Exception as e:
                print(f"Ошибка при выполнении задачи: {type(e).__name__}: {e}")
    
    if results:
        try:
            df_results = pd.DataFrame(results)
            df_results.to_excel(output_file, index=False)
        except Exception as e:
            print(f"Ошибка при записи в {output_file}: {e}")


def process_links(input_file='Links.xlsx', output_file='result.xlsx'):
    """Синхронная обертка"""
    asyncio.run(process_links_async(input_file, output_file))


if __name__ == '__main__':
    process_links()
