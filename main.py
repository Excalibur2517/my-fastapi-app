from fastapi import FastAPI, HTTPException, Query
import mysql.connector
from typing import Optional
from mysql.connector import Error


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

@app.get("/")
def root():
    return {"message": "Hello! This is Films API."}



# Эндпоинт для фильтрации фильмов по множеству параметров
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
    filters = []
    params = []

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

# 1) ЭНДПОИНТ: получить информацию о фильме по ID

@app.get("/films/{film_id}")
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
            rating_kp, 
            rating_imdb, 
            genre, 
            country, 
            name,
            description
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

# Эндпоинт для фильтрации игр по множеству параметров
@app.get("/games/advanced-filter/")
def advanced_filter(
    genre: Optional[str] = Query(None, description="Жанры игры (через запятую)"),
    platform: Optional[str] = Query(None, description="Платформы игры (через запятую)"),
    releaseDate_from: Optional[int] = Query(None, ge=1954, le=2025, description="Год выпуска от"),
    releaseDate_to: Optional[int] = Query(None, ge=1954, le=2025, description="Год выпуска до"),
    playtime_from: Optional[int] = Query(None, ge=0, le=2630, description="Время прохождения от (в часах)"),
    playtime_to: Optional[int] = Query(None, ge=0, le=2630, description="Время прохождения до (в часах)"),
    metacritic_from: Optional[int] = Query(None, ge=0, le=100, description="Metacritic рейтинг от"),
    metacritic_to: Optional[int] = Query(None, ge=0, le=100, description="Metacritic рейтинг до"),
    rawg_from: Optional[float] = Query(None, ge=0, le=5, description="Rawg рейтинг от"),
    rawg_to: Optional[float] = Query(None, ge=0, le=5, description="Rawg рейтинг до"),
    percent_from: Optional[int] = Query(None, ge=0, le=100, description="Процент рекомандации от"),
    percent_to: Optional[int] = Query(None, ge=0, le=100, description="Процент рекомандации до"),
    sort_by: Optional[str] = Query("released", description="Сортировка по: released, rating_all или popularity")
):
    filters = []
    params = []

    # Функция для установки фильтров по диапазонам
    def add_filter(field, from_value, to_value, max_value):
        if from_value is not None:
            filters.append(f"{field} >= %s")
            params.append(from_value)
        if to_value is not None:
            filters.append(f"{field} <= %s")
            params.append(to_value if to_value is not None else max_value)

    # Добавление фильтров по диапазонам
    add_filter("released", releaseDate_from, releaseDate_to, 2025)
    add_filter("playtime", playtime_from, playtime_to, 2630)
    add_filter("metacritic", metacritic_from, metacritic_to, 100)
    add_filter("rating", rawg_from, rawg_to, 5)
    add_filter("percent_recomended", percent_from, percent_to, 100)

    # Фильтрация по жанрам
    if genre:
        genre_list = genre.split(",")
        genre_placeholders = ", ".join(["%s"] * len(genre_list))
        filters.append(f"""
            games.id IN (
                SELECT DISTINCT id_game FROM games_genres_link
                WHERE id_genre IN (
                    SELECT id FROM games_genres WHERE genre IN ({genre_placeholders})
                )
            )
        """)
        params.extend(genre_list)

    # Фильтрация по платформам
    if platform:
        platform_list = platform.split(",")
        platform_placeholders = ", ".join(["%s"] * len(platform_list))
        filters.append(f"""
            games.id IN (
                SELECT DISTINCT id_game FROM games_platforms_link
                WHERE id_platform IN (
                    SELECT id FROM games_platforms WHERE platform IN ({platform_placeholders})
                )
            )
        """)
        params.extend(platform_list)

    # Проверка сортировки
    valid_sort_columns = ["released", "rating_all", "popularity"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail="Некорректное значение sort_by")

    # Финальный SQL-запрос
    query = f"""
        SELECT DISTINCT games.*
        FROM games
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