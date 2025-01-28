from fastapi import FastAPI, HTTPException, Query
import mysql.connector
from typing import Optional, List
from mysql.connector import Error
import random


# Данные для подключения к БД MySQL
db_config = {
    'host': '212.233.95.130',
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
# Эндпоинт для поиска книг по названию или автору
@app.get("/books/search_book_by_name_or_author/{search_text}", response_model=List[dict])
def search_book_by_name_or_author(search_text: str):
    """
    Ищет книги по названию или автору и возвращает до 20 первых результатов,
    отсортированных по началу совпадения, затем по популярности.
    Если запрос пустой, возвращает пустой список.
    """
    # Проверка на пустую строку
    if not search_text.strip():
        return []  # Возвращаем пустой список

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для поиска книг по названию или автору
        query = """
            SELECT id, poster_cloud, name, author
            FROM books
            WHERE name LIKE %s OR author LIKE %s
            ORDER BY 
                CASE 
                    WHEN name LIKE %s THEN 1
                    WHEN author LIKE %s THEN 2
                    ELSE 3
                END,
                popularity DESC
            LIMIT 20
        """
        # Значения для совпадения в начале текста
        start_match = f"{search_text}%"
        # Значения для частичного совпадения
        partial_match = f"%{search_text}%"
        
        cursor.execute(query, (partial_match, partial_match, start_match, start_match))
        books = cursor.fetchall()

        return books  # Возвращаем список книг
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


#Продвинутый фильтр
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
    category: Optional[str] = Query(None, description="Категории книги через запятую"),
    sort_by: Optional[str] = Query("popularity", description="Сортировка по: popularity, rating_ch, public_date или year_create")
):
    filters = []
    params = []

    def add_filter(field, from_value, to_value):
        if from_value is not None:
            filters.append(f"{field} >= %s")
            params.append(from_value)
        if to_value is not None:
            filters.append(f"{field} <= %s")
            params.append(to_value)

    add_filter("year_create", year_create_from, year_create_to)
    add_filter("rating_ch", rating_ch_from, rating_ch_to)
    add_filter("time_read", time_read_from, time_read_to)
    add_filter("public_date", public_date_from, public_date_to)

    if country_author:
        countries = country_author.split(",")
        country_conditions = " OR ".join(["country_author = %s" for _ in countries])
        filters.append(f"({country_conditions})")
        params.extend(countries)

    if age:
        ages = age.split(",")
        age_conditions = " OR ".join(["age = %s" for _ in ages])
        filters.append(f"({age_conditions})")
        params.extend(ages)

    if category:
        categories = category.split(",")
        category_conditions = " OR ".join(["bc.class_basic = %s" for _ in categories])
        filters.append(f"({category_conditions})")
        params.extend(categories)

    valid_sort_columns = ["popularity", "rating_ch", "year_create", "public_date"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail="Некорректное значение sort_by")

    query = f"""
        SELECT DISTINCT b.id, b.name, b.author, b.poster_cloud, b.popularity, b.year_create, b.rating_ch, b.time_read, b.public_date, b.country_author, b.age,
        GROUP_CONCAT(bc.class_basic) AS categories
        FROM books b
        LEFT JOIN books_catalog bc ON b.id = bc.link_id
        WHERE {" AND ".join(filters) if filters else "1=1"}
        GROUP BY b.id
        ORDER BY {sort_by} DESC
        LIMIT 100
    """

    # Debug SQL-запроса (ОПЦИОНАЛЬНО: ЛОГИРУЕТ ТОЛЬКО ЕСЛИ DEBUG ВКЛЮЧЕН)
    import os
    DEBUG = os.getenv("DEBUG_SQL", "false").lower() == "true"
    if DEBUG:
        print("Executing query:", query)
        print("With parameters:", params)

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
            b.id, 
            b.name, 
            b.author, 
            b.year_izd, 
            b.year_create, 
            b.seria, 
            b.number_pages, 
            b.country_author, 
            b.rating_ch, 
            b.poster, 
            b.age, 
            b.time_read, 
            b.description, 
            b.universe_comics, 
            b.seria_comics, 
            b.razdel_comics, 
            b.public_date, 
            b.poster_cloud, 
            b.popularity, 
            b.votes, 
            b.tirazh,
            GROUP_CONCAT(bc.class_basic) AS categories
        FROM books b
        LEFT JOIN books_catalog bc ON b.id = bc.link_id
        WHERE b.id = %s
        GROUP BY b.id
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



# Новый эндпоинт для получения фильмов по ID блока
@app.get("/books/single_block_by_id/{block_id}", response_model=List[dict])
def get_films_by_block_id(block_id: int):
    """
    Возвращает все данные из таблицы films_collections, соответствующие переданному block_id.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения данных по block_id
        cursor.execute("""
            SELECT id, blocks_id, name, poster 
            FROM books_collections 
            WHERE blocks_id = %s
        """, (block_id,))
        
        rows = cursor.fetchall()
        
        # Проверка на пустой результат
        if not rows:
            raise HTTPException(status_code=404, detail=f"No books found for block_id {block_id}")
        
        return rows
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()



# Здесь должна быть ваша функция подключения к базе данных
@app.get("/books/collections_info/{collection_id}", response_model=List[dict])
def get_films_by_collection(collection_id: int):
    """
    Возвращает всю информацию о книгах, которые относятся к указанной подборке,
    включая категории.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения информации о книгах и их категориях
        cursor.execute("""
            SELECT 
                b.id, 
                b.name, 
                b.author, 
                b.poster_cloud, 
                b.year_create, 
                b.rating_ch, 
                b.time_read, 
                b.country_author, 
                b.age, 
                GROUP_CONCAT(DISTINCT bc.class_basic) AS categories
            FROM 
                books b
            JOIN 
                books_collections_link cl ON b.id = cl.book_id
            LEFT JOIN 
                books_catalog bc ON b.id = bc.link_id
            WHERE 
                cl.collection_id = %s
            GROUP BY 
                b.id
        """, (collection_id,))
        
        films = cursor.fetchall()

        if not films:
            raise HTTPException(status_code=404, detail="Книги не найдены для данной подборки")
        
        return films
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# Эндпоинт для поиска книг
@app.get("/books/search_by_fields/")
def search_books(
    universe_comics: Optional[str] = Query(None, description="Universe Comics для фильтрации"),
    seria_comics: Optional[str] = Query(None, description="Seria Comics для фильтрации"),
    razdel_comics: Optional[str] = Query(None, description="Раздел комиксов для фильтрации"),
    seria: Optional[str] = Query(None, description="Серия для фильтрации"),
):
    filters = []
    params = []

    # Фильтры по каждому полю
    if universe_comics:
        filters.append("universe_comics = %s")
        params.append(universe_comics)
    if seria_comics:
        filters.append("seria_comics = %s")
        params.append(seria_comics)
    if razdel_comics:
        filters.append("razdel_comics = %s")
        params.append(razdel_comics)
    if seria:
        filters.append("seria = %s")
        params.append(seria)

    # Если фильтры не заданы, вернуть ошибку
    if not filters:
        raise HTTPException(status_code=400, detail="Нужно указать хотя бы один параметр для поиска.")

    # Формирование SQL-запроса
    query = f"""
        SELECT id, name, author, year_izd, year_create, seria, number_pages,
               country_author, rating_ch, poster_cloud, age, time_read, description,
               universe_comics, seria_comics, razdel_comics, public_date, popularity, votes, tirazh
        FROM books
        WHERE {" AND ".join(filters)}
        ORDER BY popularity DESC
        LIMIT 100
    """

    # Выполнение SQL-запроса
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params)
        books = cursor.fetchall()
        return books
    except Error as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Ошибка базы данных")
    finally:
        cursor.close()
        conn.close()


@app.get("/books/by_category/")
def get_books_by_category(
    class_basic: Optional[str] = Query(None, description="Основная категория"),
    class_level_2: Optional[str] = Query(None, description="Подкатегория 2-го уровня"),
    class_level_3: Optional[str] = Query(None, description="Подкатегория 3-го уровня"),
    offset: int = Query(0, ge=0, description="Смещение записей для пагинации"),
    limit: int = Query(20, ge=1, le=100, description="Количество записей для возврата"),
    sort_by: str = Query("popularity", description="Сортировка по полю: popularity, rating_ch, year_create"),
):
    """
    Эндпоинт для получения книг по категориям с пагинацией и сортировкой.
    """
    # Допустимые поля для сортировки
    valid_sort_columns = ["popularity", "rating_ch", "year_create"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail=f"Некорректное значение sort_by. Допустимые значения: {valid_sort_columns}")

    # Создаем фильтры на основе переданных параметров
    filters = []
    params = []

    if class_basic:
        filters.append("bc.class_basic = %s")
        params.append(class_basic)
    if class_level_2:
        filters.append("bc.class_level_2 = %s")
        params.append(class_level_2)
    if class_level_3:
        filters.append("bc.class_level_3 = %s")
        params.append(class_level_3)

    # Если не переданы параметры категории, возвращаем ошибку
    if not filters:
        raise HTTPException(status_code=400, detail="Необходимо указать хотя бы одну категорию.")

    # Основной SQL-запрос
    query = f"""
        SELECT DISTINCT b.id, b.name, b.author, b.poster_cloud, b.popularity, b.rating_ch, 
                        b.year_create, b.number_pages, b.country_author, b.time_read, b.age
        FROM books_catalog bc
        INNER JOIN books b ON bc.link_id = b.id
        WHERE {" AND ".join(filters)}
        ORDER BY b.{sort_by} DESC
        LIMIT %s OFFSET %s
    """

    # Добавляем лимит и смещение к параметрам
    params.extend([limit, offset])

    # Выполняем запрос к базе данных
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params)
        rows = cursor.fetchall()

        # Если ничего не найдено, возвращаем 404
        if not rows:
            raise HTTPException(status_code=404, detail="Книги не найдены для указанных категорий.")

        return rows
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
