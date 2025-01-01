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

# 2) ЭНДПОИНТ: вывести фильмы с rating_kp в заданном диапазоне
@app.get("/films/filter")
def filter_films_by_rating_kp(
    kp_from: float = Query(..., description="Минимальный рейтинг КП"),
    kp_to: float = Query(..., description="Максимальный рейтинг КП")
):
    """
    Выводит все фильмы, у которых rating_kp лежит в диапазоне [kp_from, kp_to].
    Пример: /films/filter?kp_from=6&kp_to=7
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
        WHERE rating_kp >= %s AND rating_kp <= %s
        ORDER BY rating_kp DESC
        LIMIT 50
        """
        cursor.execute(sql, (kp_from, kp_to))
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
