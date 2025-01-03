from fastapi import FastAPI, HTTPException, Query
import mysql.connector
from typing import Optional

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

# Эндпоинт для пересчета популярности
@app.post("/films/recalculate-popularity/")
def recalculate_popularity():
    conn = get_db_connection()
    cursor = conn.cursor()

    update_query = """
        UPDATE films
        SET popularity = ((rating_kp * 0.2 + rating_imdb * 0.15 + rating_critics * 0.15) * LOG10(votes_kp * 0.4 + votes_imdb * 0.3 + 1) + LOG10(boxoffice_dollar + 1))
    """

    try:
        cursor.execute(update_query)
        conn.commit()
        return {"message": "Популярность фильмов успешно пересчитана."}
    finally:
        cursor.close()
        conn.close()

# Эндпоинт для пересчета общего рейтинга
@app.post("/films/recalculate-rating-all/")
def recalculate_rating_all():
    conn = get_db_connection()
    cursor = conn.cursor()

    update_query = """
        UPDATE films
        SET rating_all = (rating_kp + rating_imdb + rating_critics) / 3
    """

    try:
        cursor.execute(update_query)
        conn.commit()
        return {"message": "Общий рейтинг фильмов успешно пересчитан."}
    finally:
        cursor.close()
        conn.close()

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
    filters = ["1=1"]
    params = []

    # Установка фильтров
    if year_from and year_to:
        filters.append("year_prem BETWEEN %s AND %s")
        params.extend([year_from, year_to])

    if kp_from and kp_to:
        filters.append("rating_kp BETWEEN %s AND %s")
        params.extend([kp_from, kp_to])

    if imdb_from and imdb_to:
        filters.append("rating_imdb BETWEEN %s AND %s")
        params.extend([imdb_from, imdb_to])

    if critics_from and critics_to:
        filters.append("rating_critics BETWEEN %s AND %s")
        params.extend([critics_from, critics_to])

    # Жанры и страны
    if genres:
        genre_list = genres.split(",")
        genre_placeholders = ", ".join(["%s"] * len(genre_list))
        filters.append(f"films.id IN (SELECT DISTINCT id_film FROM films_genre_link WHERE id_genre IN (SELECT id FROM films_genre WHERE genre IN ({genre_placeholders})))")
        params.extend(genre_list)

    if countries:
        country_list = countries.split(",")
        country_placeholders = ", ".join(["%s"] * len(country_list))
        filters.append(f"films.id IN (SELECT DISTINCT id_film FROM films_country_link WHERE id_country IN (SELECT id FROM films_country WHERE country IN ({country_placeholders})))")
        params.extend(country_list)

    # Проверка сортировки
    valid_sort_columns = ["popularity", "rating_all", "year_prem"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail="Некорректное значение sort_by")

    query = f"""
        SELECT films.*
        FROM films
        WHERE {" AND ".join(filters)}
        GROUP BY films.id
        ORDER BY {sort_by} DESC
        LIMIT 100
    """

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return rows
    finally:
        cursor.close()
        conn.close()

# Тестовый эндпоинт фильтрации по году выпуска
@app.get("/films/advanced-filter2/")
def advanced_filter(
    year_from: Optional[int] = Query(None, ge=1874, le=2025, description="Год выпуска от"),
    year_to: Optional[int] = Query(None, ge=1874, le=2025, description="Год выпуска до"),
    sort_by: Optional[str] = Query("popularity", description="Сортировка по: popularity, rating_all или year_prem")
):
    filters = ["1=1"]
    params = []

    # Проверка и установка значений для года
    if year_from is not None and year_to is not None:
        filters.append("year_prem BETWEEN %s AND %s")
        params.extend([year_from, year_to])

    # Определение сортировки
    valid_sort_columns = ["popularity", "rating_all", "year_prem"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail="Некорректное значение sort_by")

    query = f"""
        SELECT films.*
        FROM films
        WHERE {" AND ".join(filters)}
        ORDER BY {sort_by} DESC
        LIMIT 100
    """

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return rows
    finally:
        cursor.close()
        conn.close()


