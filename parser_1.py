import requests
from bs4 import BeautifulSoup
import json
import pandas as pd

# На 84 строке написан цикл, который останавливается, когда парсер собирает данные о 200 фильмах с 10 страниц
# под страницей имеется в виду страничка вики, на которой расположенны ссылки на все фильмы, которые на ней поместились, пример ниже
# пример: https://ru.wikipedia.org/w/index.php?title=%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83&pagefrom=%D0%90%D0%B2%D1%82%D0%BE%D1%80%D0%B0%21+%D0%90%D0%B2%D1%82%D0%BE%D1%80%D0%B0%21#mw-pages

# Функция для сбора данных с одной страницы
def scrape_page(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "lxml")

    # Вытащили все ссылки и положили в словарь "Название фильма": "ссылка"
    all_films_hrefs = soup.find(class_='mw-category mw-category-columns').find_all("a")

    all_hrefs_dict = {}
    for item in all_films_hrefs:
        item_text = item.text
        item_href = 'https://ru.wikipedia.org' + item.get("href")
        all_hrefs_dict[item_text] = item_href

    # Создаем пустой список для всех датафреймов
    dfs = []
    count_s = 0
    # Собираем информацию
    for count, (item_text, item_href) in enumerate(all_hrefs_dict.items()):
        revq = requests.get(url=item_href)
        ssrc = revq.text

        soup = BeautifulSoup(ssrc, "lxml")

        # Находим таблицу с информацией о фильме
        table = soup.find("table", class_="infobox")

        # Название фильма
        title = soup.find("h1", id="firstHeading").text

        # Страны
        countries = table.select('tr:contains("Стран") td')
        countries = [cell.text.strip() for cell in countries] if countries else []

        # Режиссер
        director = table.select_one('tr:contains("Режис") td')
        director = director.text.strip() if director else None

        # Жанр
        genre = table.select_one('tr:contains("Жанр") td')
        genre = genre.text.strip() if genre else None

        # Год
        year = table.select_one('tr:contains("Год") td')
        year = year.text.strip() if year else None

        # Преобразуем список стран в строку с разделителем запятая
        countries_str = ', '.join(countries) if countries else "Нет информации"

        # Создаем датафрейм и добавляем его в список
        data = {
            "Название фильма": [title],
            "Страны": [countries_str],
            "Режиссер": [director],
            "Жанр": [genre],
            "Год": [year]
        }
        
        # Добавляем счетчик чтобы не смотреть в пустой терминал
        count_s += 1
        print(f"{count_s}: {title}")

        dfs.append(pd.DataFrame(data))

    # Объединяем все датафреймы в один итоговый датафрейм
    result_df = pd.concat(dfs, ignore_index=True)

    return result_df


# Задаем список страниц для парсинга
url_start = "https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83"

link_list = []
iteration_count = 0

# Ограничение количества страниц
while iteration_count < 10:
    response = requests.get(url_start)
    soup = BeautifulSoup(response.text, "html.parser")

    link_list.append(url_start)  # Добавляем текущую ссылку в список

    next_link = soup.find("a", text="Следующая страница")

    if next_link:
        url_start = "https://ru.wikipedia.org" + next_link["href"]
    else:
        break
    
    iteration_count += 1

# Сохраняем список ссылок в JSON-файл
with open('links.json', 'w') as file:
    json.dump(link_list, file)

# Создаем пустой список для всех датафреймов
dfs = []

# Собираем данные со всех страниц
for page in link_list:
    df = scrape_page(page)
    dfs.append(df)

# Объединяем все датафреймы в один итоговый датафрейм
result_df = pd.concat(dfs, ignore_index=True)
result_df.to_csv('result.csv', index=False)

print("\nРезультат сохранен в файл result.csv")

# print(result_df)
