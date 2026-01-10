import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urlparse, urljoin
import re


def extract_ship_from_details_page(url, headers):
    """
    Извлекает информацию о судне со страницы деталей VesselFinder
    
    Args:
        url: URL страницы деталей
        headers: HTTP заголовки
        
    Returns:
        dict: Словарь с информацией о судне или None
    """
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        ship_data = {}
        
        # Извлекаем название из заголовка h1
        h1 = soup.find('h1')
        if h1:
            ship_data['Name'] = h1.get_text(strip=True)
        
        # Извлекаем IMO и тип из заголовка h2 или текста
        h2 = soup.find('h2')
        if h2:
            h2_text = h2.get_text()
            imo_match = re.search(r'IMO\s*(\d+)', h2_text, re.IGNORECASE)
            if imo_match:
                ship_data['IMO'] = imo_match.group(1)
            
            # Извлекаем тип из h2 (формат: "Crude Oil Tanker, IMO 9431381")
            # Тип идет до запятой
            parts = h2_text.split(',')
            if parts:
                type_candidate = parts[0].strip()
                # Проверяем что это не пусто и не содержит IMO
                if type_candidate and 'IMO' not in type_candidate.upper():
                    ship_data['Type'] = type_candidate
        
        # Ищем MMSI, IMO и тип в таблицах
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    
                    if 'mmsi' in label.lower():
                        mmsi_match = re.search(r'\d+', value)
                        if mmsi_match:
                            ship_data['MMSI'] = mmsi_match.group(0)
                    
                    if 'imo' in label.lower() and 'IMO' not in ship_data:
                        imo_match = re.search(r'\d+', value)
                        if imo_match:
                            ship_data['IMO'] = imo_match.group(0)
                    
                    if ('type' in label.lower() or 'вид' in label.lower() or 'тип' in label.lower()) and 'Type' not in ship_data:
                        ship_data['Type'] = value
        
        # Ищем в div-контейнерах с данными
        info_divs = soup.find_all('div', class_=re.compile(r'info|data|detail', re.IGNORECASE))
        for div in info_divs:
            div_text = div.get_text()
            if 'mmsi' in div_text.lower() and 'MMSI' not in ship_data:
                mmsi_match = re.search(r'MMSI[:\s]*(\d+)', div_text, re.IGNORECASE)
                if mmsi_match:
                    ship_data['MMSI'] = mmsi_match.group(1)
        
        # Если MMSI не найден, ищем в тексте всей страницы
        if 'MMSI' not in ship_data:
            page_text = soup.get_text()
            mmsi_match = re.search(r'MMSI[:\s]*(\d+)', page_text, re.IGNORECASE)
            if mmsi_match:
                ship_data['MMSI'] = mmsi_match.group(1)
        
        # Проверяем что нашли хотя бы основные данные
        if ship_data.get('Name') or ship_data.get('IMO'):
            return {
                'Name': ship_data.get('Name', 'N/A'),
                'IMO': ship_data.get('IMO', 'N/A'),
                'MMSI': ship_data.get('MMSI', 'N/A'),
                'Type': ship_data.get('Type', 'N/A')
            }
        
        return None
        
    except Exception as e:
        print(f"  Ошибка при извлечении данных со страницы деталей: {e}")
        return None


def extract_ship_info(url):
    """
    Извлекает информацию о судах со страницы VesselFinder
    
    Returns:
        list: Список словарей с информацией о судах (0, 1 или более), или None если ошибка
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Проверяем, это страница поиска или страница деталей
        if '/vessels/details/' in url:
            # Это страница деталей
            ship = extract_ship_from_details_page(url, headers)
            return [ship] if ship else []
        
        # Это страница поиска - определяем количество результатов
        # Подсчитываем количество ссылок на страницы деталей
        detail_links = soup.find_all('a', href=re.compile(r'/vessels/details/\d+'))
        ship_count = len(detail_links)
        
        # Если не нашли ссылки, пробуем найти количество из текста
        if ship_count == 0:
            page_text = soup.get_text()
            # Ищем текст с количеством судов (например, "1 судно", "2 судна", "10 судов")
            count_match = re.search(r'(\d+)\s+судно', page_text, re.IGNORECASE)
            
            if not count_match:
                # Пробуем английский вариант
                count_match = re.search(r'(\d+)\s+vessel', page_text, re.IGNORECASE)
            
            if count_match:
                ship_count = int(count_match.group(1))
            else:
                # Подсчитываем количество строк в таблице результатов
                table = soup.find('table')
                if table:
                    rows = table.find_all('tr')
                    # Ищем строки с ссылками на детали
                    ship_count = len([r for r in rows if r.find('a', href=re.compile(r'/vessels/details/'))])
        
        # Если судов не найдено
        if ship_count == 0:
            return []
        
        # Если судов больше одного - возвращаем список с пустыми данными для подсчета
        if ship_count > 1:
            # Возвращаем список с нужным количеством элементов
            return [None] * ship_count
        
        # Если ровно одно судно - переходим на страницу деталей
        if ship_count == 1:
            # Используем найденную ранее ссылку или ищем снова
            details_link = None
            if detail_links:
                details_link = detail_links[0]
            else:
                details_link = soup.find('a', href=re.compile(r'/vessels/details/\d+'))
            
            if details_link and details_link.get('href'):
                details_url = urljoin(url, details_link['href'])
                ship = extract_ship_from_details_page(details_url, headers)
                return [ship] if ship else []
        
        return []
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            print(f"  HTTP 400: Неверный запрос")
        return None
    except Exception as e:
        print(f"  Ошибка: {e}")
        return None


def process_links(input_file='Links.xlsx', output_file='result.xlsx'):
    """
    Обрабатывает ссылки из файла и записывает результаты в Excel
    
    Args:
        input_file: Файл со ссылками
        output_file: Файл для записи результатов
    """
    # Читаем ссылки из Excel
    try:
        df_links = pd.read_excel(input_file)
        print(f"Загружено {len(df_links)} ссылок из {input_file}")
        
        # Определяем колонку со ссылками
        link_column = None
        for col in df_links.columns:
            if 'link' in str(col).lower() or 'url' in str(col).lower() or 'http' in str(df_links[col].iloc[0]).lower():
                link_column = col
                break
        
        if link_column is None:
            # Берем первую колонку
            link_column = df_links.columns[0]
        
        print(f"Используется колонка: {link_column}")
        
    except Exception as e:
        print(f"Ошибка при чтении файла {input_file}: {e}")
        return
    
    # Результаты
    results = []
    
    # Обрабатываем каждую ссылку
    for idx, row in df_links.iterrows():
        url = str(row[link_column]).strip()
        
        if not url or not url.startswith('http'):
            print(f"Пропущена невалидная ссылка в строке {idx + 1}: {url}")
            continue
        
        print(f"\nОбработка ссылки {idx + 1}/{len(df_links)}: {url}")
        ships = extract_ship_info(url)
        
        if ships is None:
            print(f"  -> Ошибка или данные не найдены")
            continue
        
        if len(ships) == 0:
            print(f"  -> Судов не найдено, пропускаем")
            continue
        elif len(ships) > 1:
            print(f"  -> Найдено {len(ships)} судов, пропускаем")
            continue
        else:
            # Найдено ровно одно судно
            ship = ships[0]
            print(f"  -> Найдено судно: {ship['Name']} (IMO: {ship['IMO']}, MMSI: {ship['MMSI']})")
            results.append(ship)
        
        # Небольшая задержка между запросами
        time.sleep(1)
    
    # Записываем результаты в Excel
    if results:
        df_results = pd.DataFrame(results)
        df_results.to_excel(output_file, index=False)
        print(f"\nРезультаты записаны в {output_file}")
        print(f"Найдено и обработано судов: {len(results)}")
    else:
        print("\nНе найдено судов для записи")


if __name__ == '__main__':
    process_links()
