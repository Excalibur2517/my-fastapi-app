from fastapi import FastAPI, HTTPException, Query
import mysql.connector
from typing import Optional, List
from mysql.connector import Error
import random


# Данные для подключения к БД MySQL
db_config = {
    'host': '83.166.232.67',
    'user': 'test_user',
    'password': 'Projectbd12345.!',
    'database': 'MySQL-6549'
}

app = FastAPI(title="Films API")

# Функция для получения нового соединения с БД
def get_db_connection():
    conn = mysql.connector.connect(**db_config)
    return conn

# Главная страница
@app.get("/")
def root():
    return {"message": "Hello! This is Films API."}


#
# Главная страница
# Эндпоинт для получения 20 случайных фильмов из топ-200 по популярности
# Эндпоинт для получения 20 случайных фильмов из топ-200 по популярности
@app.get("/films/random_top200/")
def get_random_top_200_films():
    """
    Возвращает 20 случайных фильмов из топ-200 по популярности.
    Поля: id, name, poster_cloud.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # Выбираем топ-200 фильмов по популярности
        cursor.execute("""
            SELECT id, name, poster_cloud
            FROM films
            WHERE m_or_ser = 'movie'
            AND CHAR_LENGTH(name) <= 25
            ORDER BY popularity DESC
            LIMIT 300
        """)
        films = cursor.fetchall()

        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены")

        # Берем 20 случайных фильмов из списка топ-200
        random_films = random.sample(films, 20)

        return random_films
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"Ошибка выполнения SQL-запроса: {err}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Неизвестная ошибка: {e}")
    finally:
        cursor.close()
        conn.close()
# 🚀 Новый эндпоинт для получения фильмов с главного экрана
@app.get("/films/main_screen_movies", response_model=List[dict])
def get_main_screen_films():
    """
    Получает список фильмов с главного экрана по ID из таблицы films_main_screen
    и возвращает их имя, постер и ID.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения ID фильмов с главного экрана
        cursor.execute("""
            SELECT f.id, f.name, f.poster_cloud
            FROM films_main_screen ms
            JOIN films f ON ms.id = f.id
        """)
        rows = cursor.fetchall()
        
        if not rows:
            raise HTTPException(status_code=404, detail="No films found")
        
        return rows
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.get("/films/advanced-filter/")
def advanced_filter(
    genres: Optional[str] = Query(None, description="Жанры фильма (через запятую)"),
    countries: Optional[str] = Query(None, description="Страны производства (через запятую)"),
    year_from: Optional[int] = Query(None, ge=1874, le=2025, description="Год выпуска от"),
    year_to: Optional[int] = Query(None, ge=1874, le=2025, description="Год выпуска до"),
    kp_from: Optional[float] = Query(None, ge=0, le=10, description="Рейтинг КП от"),
    kp_to: Optional[float] = Query(None, ge=0, le=10, description="Рейтинг КП до"),
    imdb_from: Optional[float] = Query(None, ge=0, le=10, description="Рейтинг IMDb от"),
    imdb_to: Optional[float] = Query(None, ge=0, le=10, description="Рейтинг IMDb до"),
    critics_from: Optional[float] = Query(None, ge=0, le=10, description="Рейтинг критиков от"),
    critics_to: Optional[float] = Query(None, ge=0, le=10, description="Рейтинг критиков до"),
    boxoffice_from: Optional[int] = Query(None, ge=0, le=2923710000, description="Сборы от"),
    boxoffice_to: Optional[int] = Query(None, ge=0, le=2923710000, description="Сборы до"),
    budget_from: Optional[int] = Query(None, ge=0, le=400000000, description="Бюджет от"),
    budget_to: Optional[int] = Query(None, ge=0, le=400000000, description="Бюджет до"),
    duration_from: Optional[int] = Query(None, ge=0, le=600, description="Длительность от"),
    duration_to: Optional[int] = Query(None, ge=0, le=600, description="Длительность до"),
    age_from: Optional[int] = Query(None, ge=0, le=18, description="Возрастное ограничение от"),
    age_to: Optional[int] = Query(None, ge=0, le=18, description="Возрастное ограничение до"),
    sort_by: Optional[str] = Query("popularity", description="Сортировка по: popularity, rating_all или year_prem")
):
    filters = ["m_or_ser = 'movie'"]  # Только фильмы
    params = []

    # Исключение короткометражек по умолчанию
    if not genres or "Короткометражка" not in genres:
        filters.append("films.id NOT IN (SELECT id_film FROM films_genre_link WHERE id_genre IN (SELECT id FROM films_genre WHERE genre = 'Короткометражка'))")

    # Установка фильтров по диапазонам
    def add_filter(field, from_value, to_value):
        if from_value is not None:
            filters.append(f"{field} >= %s")
            params.append(from_value)
        if to_value is not None:
            filters.append(f"{field} <= %s")
            params.append(to_value)

    # Добавление фильтров
    add_filter("year_prem", year_from, year_to)
    add_filter("rating_kp", kp_from, kp_to)
    add_filter("rating_imdb", imdb_from, imdb_to)
    add_filter("rating_critics", critics_from, critics_to)
    add_filter("boxoffice_dollar", boxoffice_from, boxoffice_to)
    add_filter("budget_dollar", budget_from, budget_to)
    add_filter("timing_m", duration_from, duration_to)
    add_filter("age", age_from, age_to)

    # Фильтрация по жанрам
    if genres:
        genre_list = genres.split(",")
        genre_placeholders = ", ".join(["%s"] * len(genre_list))
        filters.append(f"films.id IN (SELECT DISTINCT id_film FROM films_genre_link WHERE id_genre IN (SELECT id FROM films_genre WHERE genre IN ({genre_placeholders})))")
        params.extend(genre_list)

    # Фильтрация по странам
    if countries:
        country_list = countries.split(",")
        country_placeholders = ", ".join(["%s"] * len(country_list))
        filters.append(f"films.id IN (SELECT DISTINCT id_film FROM films_country_link WHERE id_country IN (SELECT id FROM films_country WHERE country IN ({country_placeholders})))")
        params.extend(country_list)

    # Проверка сортировки
    valid_sort_columns = ["popularity", "rating_all", "year_prem"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail="Некорректное значение sort_by")

    # Финальный SQL-запрос
    query = f"""
        SELECT DISTINCT films.*
        FROM films
        WHERE {" AND ".join(filters)}
        ORDER BY {sort_by} DESC
        LIMIT 100
    """

    # Выполнение запроса
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return rows
    finally:
        cursor.close()
        conn.close()


# Эндпоинт: получить информацию о фильме по ID
@app.get("/films/search_film_by_id/{film_id}")
def get_film_by_id(film_id: int):
    """
    Возвращает всю информацию о фильме (id, rating_kp, rating_imdb, genre, country, name, description)
    по указанному film_id.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        sql = """
        SELECT 
            id, 
            boxoffice_dollar,
            rating_kp, 
            rating_imdb,
            rating_critics,
            year_prem,
            poster_cloud, 
            genre, 
            country, 
            name,
            description,
            directors,
            actors,
            budget_dollar,
            age
        FROM films
        WHERE id = %s
        """
        cursor.execute(sql, (film_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Film not found")
        return row
    finally:
        cursor.close()
        conn.close()

# Новый эндпоинт для получения всех блоков подборок
@app.get("/films/blocks_of_collection/", response_model=List[dict])
def get_films_collections():
    """
    Возвращает все строки из таблицы films_collection_blocks с полями name и poster.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения данных из таблицы
        cursor.execute("SELECT id,name, poster FROM films_collection_blocks")
        rows = cursor.fetchall()
        
        # Проверка на пустой результат
        if not rows:
            raise HTTPException(status_code=404, detail="No collection blocks found")
        
        return rows
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# Новый эндпоинт для получения фильмов по ID блока
@app.get("/films/single_block_by_id/{block_id}", response_model=List[dict])
def get_films_by_block_id(block_id: int):
    """
    Возвращает все данные из таблицы films_collections, соответствующие переданному block_id.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения данных по block_id
        cursor.execute("""
            SELECT id, block_id, name, poster 
            FROM films_collections 
            WHERE block_id = %s
        """, (block_id,))
        
        rows = cursor.fetchall()
        
        # Проверка на пустой результат
        if not rows:
            raise HTTPException(status_code=404, detail=f"No films found for block_id {block_id}")
        
        return rows
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# Новый эндпоинт для получения 10 фильмов с наименьшим количеством символов в name
@app.get("/films/10_shortest_collections_list/", response_model=List[dict])
def get_shortest_names():
    """
    Возвращает 10 фильмов с наименьшим количеством символов в поле name.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения 10 строк с наименьшим количеством символов в поле name
        cursor.execute("""
            SELECT id, name, poster 
            FROM films_collections 
            ORDER BY LENGTH(name) ASC
            LIMIT 10
        """)
        
        rows = cursor.fetchall()
        
        # Проверка на пустой результат
        if not rows:
            raise HTTPException(status_code=404, detail="No films found")
        
        return rows
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# Эндпоинт для получения фильмов по ID подборки
@app.get("/films/collections_info/{collection_id}", response_model=List[dict])
def get_films_by_collection(collection_id: int):
    """
    Возвращает всю информацию о фильмах, которые относятся к указанной подборке.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Исправленный SQL-запрос для получения фильмов по ID подборки
        cursor.execute("""
            SELECT f.id,f.name,f.country,f.rating_kp,f.rating_imdb,f.rating_critics,f.genre,f.poster_cloud,f.year_prem
            FROM films f
            JOIN films_collection_link cl ON f.id = cl.films_id
            WHERE cl.collection_id = %s
        """, (collection_id,))
        
        films = cursor.fetchall()

        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены для данной подборки")
        
        return films
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# Эндпоинт для поиска фильмов по названию
@app.get("/films/search_film_by_name/{search_text}", response_model=List[dict])
def search_film_by_name(search_text: str):
    """
    Ищет фильмы по названию и возвращает 20 первых результатов, отсортированных по popularity.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для поиска фильмов по названию
        query = """
            SELECT id, name, poster_cloud, popularity,rating_kp,rating_imdb,rating_critics,genre,country,year_prem
            FROM films
            WHERE m_or_ser = 'movie' AND name LIKE %s
            ORDER BY popularity DESC
            LIMIT 20
        """
        cursor.execute(query, (f"%{search_text}%",))
        films = cursor.fetchall()

        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены")
        
        return films
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# Новый эндпоинт для получения фильмов по жанру
@app.get("/films/by_genre/")
def get_films_by_genre(
    genre: str,
    offset: int = 0,
    limit: int = 20,
    sort_by: str = "popularity"
):
    """
    Возвращает фильмы по выбранному жанру с возможностью пагинации и сортировки.
    Исключает фильмы жанра 'Короткометражка', если они не выбраны явно.
    """
    # Проверка на допустимые значения sort_by
    valid_sort_columns = ["popularity", "rating_all", "year_prem"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail=f"Некорректное значение sort_by. Возможные значения: {valid_sort_columns}")

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Определяем фильтр для жанра
        query = f"""
            SELECT DISTINCT f.id, f.name, f.poster_cloud, f.popularity, f.rating_kp, f.rating_imdb, 
                            f.rating_critics, f.genre, f.country, f.year_prem, f.rating_all
            FROM films f
            JOIN films_genre_link fgl ON f.id = fgl.id_film
            JOIN films_genre fg ON fgl.id_genre = fg.id
            WHERE fg.genre = %s
            AND f.m_or_ser = 'movie' AND year_prem < 2025
        """

        # Исключение короткометражек, если жанр не "Короткометражка"
        if genre.lower() != "короткометражка":
            query += """
                AND f.id NOT IN (
                    SELECT id_film
                    FROM films_genre_link
                    WHERE id_genre = (SELECT id FROM films_genre WHERE genre = 'Короткометражка')
                )
            """

        # Добавляем сортировку, лимит и офсет
        query += f" ORDER BY f.{sort_by} DESC LIMIT %s OFFSET %s"

        # Выполняем запрос
        cursor.execute(query, (genre, limit, offset))
        rows = cursor.fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="Фильмы не найдены для указанного жанра")

        return rows
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
