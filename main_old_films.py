from fastapi import FastAPI, HTTPException, Query
import mysql.connector
from typing import Optional, List
from mysql.connector import Error
import random


# Данные для подключения к БД MySQL
db_config = {
    'host': '217.16.26.244',
    'user': 'user_1',
    'password': 'Projectbd170898.!',
    'database': 'MySQL-2575'
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


#------------------------------------КНИГИ----------------------------------------------
# Новый эндпоинт для получения КАТАЛОГ 1
#------------------------------------КНИГИ----------------------------------------------
# Новый эндпоинт для получения КАТАЛОГ 1
@app.get("/books/classes")
def get_distinct_classes():
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        # Выполняем запрос
        query = """
        SELECT DISTINCT class_basic, class_level_2, class_level_3
        FROM books_catalog bc
        """
        cursor.execute(query)
        
        # Получаем результаты
        results = cursor.fetchall()
        classes = [
            {
                "class_basic": row[0],
                "class_level_2": row[1],
                "class_level_3": row[2]
            }
            for row in results
        ]

        # Закрываем соединение
        cursor.close()
        conn.close()

        # Возвращаем результаты
        return {"classes": classes}

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при работе с базой данных: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Неожиданная ошибка: {str(e)}")


# Новый эндпоинт для получения 10 фильмов с наименьшим количеством символов в name
@app.get("/books/10_shortest_collections_list/", response_model=List[dict])
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
            FROM books_collections 
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

# Эндпоинт для поиска книг по названию
@app.get("/books/search_book_by_name_or_author/{search_text}", response_model=List[dict])
def search_book_by_name_or_author(search_text: str):
    """
    Ищет книги по названию или автору и возвращает до 20 первых результатов,
    отсортированных по популярности.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для поиска книг по названию или автору
        query = """
            SELECT id, poster_cloud, name, author
            FROM books
            WHERE name LIKE %s OR author LIKE %s
            ORDER BY popularity DESC
            LIMIT 20
        """
        cursor.execute(query, (f"%{search_text}%", f"%{search_text}%"))
        books = cursor.fetchall()

        if not books:
            raise HTTPException(status_code=404, detail="Книги не найдены")
        
        return books
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# Эндпоинт для поиска книг по фильтрам
@app.get("/books/advanced-filter/")
def advanced_filter(
    year_create_from: Optional[int] = Query(None, ge=-800, le=2024, description="Год создания книги от"),
    year_create_to: Optional[int] = Query(None, ge=-800, le=2024, description="Год создания книги до"),
    country_author: Optional[str] = Query(None, description="Страна автора"),
    rating_ch_from: Optional[float] = Query(None, ge=0, le=5, description="Рейтинг Читай-город от"),
    rating_ch_to: Optional[float] = Query(None, ge=0, le=5, description="Рейтинг Читай-город до"),
    age: Optional[str] = Query(None, description="Возрастное ограничение (через запятую)"),
    time_read_from: Optional[int] = Query(None, ge=0, le=200, description="Время чтения от (часов)"),
    time_read_to: Optional[int] = Query(None, ge=0, le=200, description="Время чтения до (часов)"),
    public_date_from: Optional[int] = Query(None, ge=1600, le=2024, description="Дата публикации от"),
    public_date_to: Optional[int] = Query(None, ge=1600, le=2024, description="Дата публикации до"),
    sort_by: Optional[str] = Query("popularity", description="Сортировка по: popularity, rating_ch или public_date")
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
    add_filter("year_create", year_create_from, year_create_to)
    add_filter("rating_ch", rating_ch_from, rating_ch_to)
    add_filter("time_read", time_read_from, time_read_to)
    add_filter("public_date", public_date_from, public_date_to)

    # Фильтрация по стране автора
    if country_author:
        filters.append("country_author = %s")
        params.append(country_author)

    # Фильтрация по возрастному ограничению
    if age:
        age_list = age.split(",")
        age_placeholders = ", ".join(["%s"] * len(age_list))
        filters.append(f"age IN ({age_placeholders})")
        params.extend(age_list)

    # Проверка сортировки
    valid_sort_columns = ["popularity", "rating_ch", "public_date"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail="Некорректное значение sort_by")

    # Финальный SQL-запрос
    query = f"""
        SELECT DISTINCT books.*
        FROM books
        WHERE {" AND ".join(filters)}
        ORDER BY {sort_by} DESC
        LIMIT 100
    """

    # Выполнение запроса
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        print("SQL Query:", query)
        print("Parameters:", params)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        print("Error executing query:", str(e))
        raise HTTPException(status_code=500, detail=f"Ошибка выполнения запроса: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# Функция для получения соединения с базой данных (пример, настройте под свои нужды)
def get_db_connection():
    import mysql.connector
    return mysql.connector.connect(
        host="localhost",
        user="your_user",
        password="your_password",
        database="your_database"
    )
