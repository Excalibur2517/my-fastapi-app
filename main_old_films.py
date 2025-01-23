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

@app.get("/books/advanced-filter/")
def advanced_filter(
    year_create_from: Optional[int] = Query(None, ge=-800, le=2024, description="Год создания книги от"),
    year_create_to: Optional[int] = Query(None, ge=-800, le=2024, description="Год создания книги до"),
    country_author: Optional[str] = Query(None, description="Страны автора через запятую"),
    rating_ch_from: Optional[float] = Query(None, ge=0, le=5, description="Рейтинг Читай-город от"),
    rating_ch_to: Optional[float] = Query(None, ge=0, le=5, description="Рейтинг Читай-город до"),
    age: Optional[str] = Query(None, description="Возрастные ограничения через запятую"),
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

    # Фильтрация по странам автора
    if country_author:
        countries = country_author.split(",")
        country_conditions = " OR ".join(["country_author = %s" for _ in countries])
        filters.append(f"({country_conditions})")
        params.extend(countries)

    # Фильтрация по возрастным ограничениям
    if age:
        ages = age.split(",")
        age_conditions = " OR ".join(["age = %s" for _ in ages])
        filters.append(f"({age_conditions})")
        params.extend(ages)

    # Проверка сортировки
    valid_sort_columns = ["popularity", "rating_ch", "year_create"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail="Некорректное значение sort_by")

    # Финальный SQL-запрос
    query = f"""
        SELECT DISTINCT id, name,author, poster_cloud, popularity, year_create, rating_ch, time_read, public_date, country_author, age
        FROM books
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
    except Exception as e:
        print("Error executing query:", str(e))
        raise HTTPException(status_code=500, detail=f"Ошибка выполнения запроса: {str(e)}")
    finally:
        cursor.close()
        conn.close()


#Поиск по фильтрам
@app.get("/books/search_book_by_id/{book_id}")
def get_book_by_id(book_id: int):
    """
    Возвращает информацию о книге (id, name, author, year_izd, year_create, seria,
    number_pages, country_author, rating_ch, poster, age, time_read, description,
    universe_comics, seria_comics, razdel_comics, public_date, poster_cloud,
    popularity, votes, tirazh) по указанному book_id.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        sql = """
        SELECT 
            id, 
            name, 
            author, 
            year_izd, 
            year_create, 
            seria, 
            number_pages, 
            country_author, 
            rating_ch, 
            poster, 
            age, 
            time_read, 
            description, 
            universe_comics, 
            seria_comics, 
            razdel_comics, 
            public_date, 
            poster_cloud, 
            popularity, 
            votes, 
            tirazh
        FROM books
        WHERE id = %s
        """
        cursor.execute(sql, (book_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Book not found")
        return row
    finally:
        cursor.close()
        conn.close()



# Новый эндпоинт для получения всех блоков подборок
@app.get("/books/blocks_of_collection/", response_model=List[dict])
def get_films_collections():
    """
    Возвращает все строки из таблицы films_collection_blocks с полями name и poster.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения данных из таблицы
        cursor.execute("SELECT id,name, poster FROM books_collection_blocks")
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