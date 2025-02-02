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
    return {"message": "Hello! This is Games API."}

#----------------------------ИГРЫ-----------------------------------------------------------------

@app.get("/game/search_game_by_name/{search_text}", response_model=List[dict])
def search_film_by_name(search_text: str):
    """
    Ищет фильмы по названию и возвращает 20 первых результатов,
    отсортированных по началу совпадения, затем по популярности.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для поиска фильмов по названию
        query = """
            SELECT id, name, poster_cloud
            FROM games
            WHERE  name LIKE %s
            ORDER BY 
                CASE 
                    WHEN name LIKE %s THEN 1
                    ELSE 2
                END,
                popularity DESC
            LIMIT 20
        """
        # Значения для совпадения
        start_match = f"{search_text}%"  # Совпадение в начале текста
        partial_match = f"%{search_text}%"  # Частичное совпадение

        cursor.execute(query, (partial_match, start_match))
        films = cursor.fetchall()

        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены")
        
        return films
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/game/random_top200/")
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
            SELECT g.id, g.name, g.poster_cloud, g.popularity
FROM games g
JOIN games_platforms_link gpl ON g.id = gpl.id_game
JOIN games_platforms gp ON gpl.id_platform = gp.id
WHERE CHAR_LENGTH(g.name) <= 25
AND (gp.platform LIKE '%PC%' OR gp.platform LIKE '%PlayStation 5%' OR gp.platform LIKE '%PlayStation 4%' OR gp.platform LIKE '%Xbox One%' OR gp.platform LIKE '%Xbox Series S/X%' )
AND g.poster_cloud IS NOT NULL
AND g.poster_cloud <> ''
GROUP BY g.id, g.name, g.poster_cloud, g.popularity
ORDER BY g.popularity DESC
LIMIT 300;
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

@app.get("/games/search_game_by_id/{game_id}")
def get_game_by_id(game_id: int):
    """
    Возвращает всю информацию об игре (id, metacritic, rating, name, genre, platforms, description, released, playtime, poster_cloud, popularity)
    по указанному game_id.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        sql = """
        SELECT 
            id, 
            metacritic,
            rating,
            name,
            genre, 
            platforms, 
            description,
            released,
            playtime,
            poster_cloud,
            popularity,
            rating_count,
            percent_recommended,
            popularity,
            tags
        FROM games
        WHERE id = %s
        """
        cursor.execute(sql, (game_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Game not found")
        return row
    finally:
        cursor.close()
        conn.close()

# Новый эндпоинт для получения 10 фильмов с наименьшим количеством символов в name
@app.get("/game/10_shortest_collections_list_PC/", response_model=List[dict])
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
            FROM PC_collections 
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

# Новый эндпоинт для получения 10 фильмов с наименьшим количеством символов в name
@app.get("/game/10_shortest_collections_list_PS/", response_model=List[dict])
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
            FROM PS_collections 
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

# Новый эндпоинт для получения 10 фильмов с наименьшим количеством символов в name
@app.get("/game/10_shortest_collections_list_XBOX/", response_model=List[dict])
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
            FROM XBOX_collections 
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

# Новый эндпоинт для получения всех блоков подборок
@app.get("/game/blocks_of_collection_PC/", response_model=List[dict])
def get_films_collections():
    """
    Возвращает все строки из таблицы films_collection_blocks с полями name и poster.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения данных из таблицы
        cursor.execute("SELECT id,name, poster FROM PC_collection_blocks")
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

# Новый эндпоинт для получения всех блоков подборок
@app.get("/game/blocks_of_collection_PS/", response_model=List[dict])
def get_films_collections():
    """
    Возвращает все строки из таблицы films_collection_blocks с полями name и poster.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения данных из таблицы
        cursor.execute("SELECT id,name, poster FROM PS_collection_blocks")
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

# Новый эндпоинт для получения всех блоков подборок
@app.get("/game/blocks_of_collection_XBOX/", response_model=List[dict])
def get_films_collections():
    """
    Возвращает все строки из таблицы films_collection_blocks с полями name и poster.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения данных из таблицы
        cursor.execute("SELECT id,name, poster FROM XBOX_collection_blocks")
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
@app.get("/game/single_block_by_id_PC/{block_id}", response_model=List[dict])
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
            FROM PC_collections 
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

# Новый эндпоинт для получения фильмов по ID блока
@app.get("/game/single_block_by_id_PS/{block_id}", response_model=List[dict])
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
            FROM PS_collections 
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

# Новый эндпоинт для получения фильмов по ID блока
@app.get("/game/single_block_by_id_XBOX/{block_id}", response_model=List[dict])
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
            FROM XBOX_collections 
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


@app.get("/game/collections_info_PC/{collection_id}", response_model=List[dict])
def get_games_by_collection(collection_id: int):
    """
    Возвращает всю информацию об играх, которые относятся к указанной подборке.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения игр по ID подборки
        cursor.execute("""
            SELECT 
                g.id,
                g.name,
                g.genre,
                g.platforms,
                g.rating,
                g.rating_all,
                g.metacritic,
                g.popularity,
                g.percent_recommended,
                g.poster_cloud,
                g.released,
                g.playtime
            FROM games g
            JOIN PC_collections_link pcl ON g.id = pcl.game_id
            WHERE pcl.collection_id = %s
        """, (collection_id,))
        
        games = cursor.fetchall()

        if not games:
            raise HTTPException(status_code=404, detail="Игры не найдены для данной подборки")
        
        return games
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/game/collections_info_PS/{collection_id}", response_model=List[dict])
def get_games_by_collection(collection_id: int):
    """
    Возвращает всю информацию об играх, которые относятся к указанной подборке.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения игр по ID подборки
        cursor.execute("""
            SELECT 
                g.id,
                g.name,
                g.genre,
                g.platforms,
                g.rating,
                g.rating_all,
                g.metacritic,
                g.popularity,
                g.percent_recommended,
                g.poster_cloud,
                g.released,
                g.playtime
            FROM games g
            JOIN PS_collections_link pcl ON g.id = pcl.game_id
            WHERE pcl.collection_id = %s
        """, (collection_id,))
        
        games = cursor.fetchall()

        if not games:
            raise HTTPException(status_code=404, detail="Игры не найдены для данной подборки")
        
        return games
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/game/collections_info_XBOX/{collection_id}", response_model=List[dict])
def get_games_by_collection(collection_id: int):
    """
    Возвращает всю информацию об играх, которые относятся к указанной подборке.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения игр по ID подборки
        cursor.execute("""
            SELECT 
                g.id,
                g.name,
                g.genre,
                g.platforms,
                g.rating,
                g.rating_all,
                g.metacritic,
                g.popularity,
                g.percent_recommended,
                g.poster_cloud,
                g.released,
                g.playtime
            FROM games g
            JOIN XBOX_collections_link pcl ON g.id = pcl.game_id
            WHERE pcl.collection_id = %s
        """, (collection_id,))
        
        games = cursor.fetchall()

        if not games:
            raise HTTPException(status_code=404, detail="Игры не найдены для данной подборки")
        
        return games
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
#----------------------------ИГРЫ IOS ANDROID-----------------------------------------------------------------
@app.get("/gameios/random_top200/")
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
            SELECT g.id, g.name, g.poster_cloud, g.popularity
FROM games g
JOIN games_platforms_link gpl ON g.id = gpl.id_game
JOIN games_platforms gp ON gpl.id_platform = gp.id
WHERE CHAR_LENGTH(g.name) <= 25
AND (gp.platform LIKE '%iOS%' OR gp.platform LIKE '%Android 5%' )
AND g.poster_cloud IS NOT NULL
AND g.poster_cloud <> ''
GROUP BY g.id, g.name, g.poster_cloud, g.popularity
ORDER BY g.popularity DESC
LIMIT 300;
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

# Новый эндпоинт для получения 10 фильмов с наименьшим количеством символов в name
@app.get("/game/10_shortest_collections_list_IOS_Android/", response_model=List[dict])
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
            FROM IOS_ANDROID_collections 
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

# Новый эндпоинт для получения всех блоков подборок
@app.get("/game/blocks_of_collection_IOS_AN/", response_model=List[dict])
def get_films_collections():
    """
    Возвращает все строки из таблицы films_collection_blocks с полями name и poster.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения данных из таблицы
        cursor.execute("SELECT id,name, poster FROM IOS_ANDROID_collection_blocks")
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
@app.get("/game/single_block_by_id_IOS_AN/{block_id}", response_model=List[dict])
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
            FROM IOS_ANDROID_collections 
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

@app.get("/game/collections_info_IOS_AN/{collection_id}", response_model=List[dict])
def get_games_by_collection(collection_id: int):
    """
    Возвращает всю информацию об играх, которые относятся к указанной подборке.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # SQL-запрос для получения игр по ID подборки
        cursor.execute("""
            SELECT 
                g.id,
                g.name,
                g.genre,
                g.platforms,
                g.rating,
                g.rating_all,
                g.metacritic,
                g.popularity,
                g.percent_recommended,
                g.poster_cloud,
                g.released,
                g.playtime
            FROM games g
            JOIN IOS_ANDROID_collections_link pcl ON g.id = pcl.game_id
            WHERE pcl.collection_id = %s
        """, (collection_id,))
        
        games = cursor.fetchall()

        if not games:
            raise HTTPException(status_code=404, detail="Игры не найдены для данной подборки")
        
        return games
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.get("/games/advanced-filter/", response_model=List[dict])
def advanced_filter_games(
    genres: Optional[str] = Query(None, description="Жанры игры (через запятую)"),
    platforms: Optional[str] = Query(None, description="Платформы игры (через запятую)"),
    release_from: Optional[int] = Query(None, ge=1954, le=2024, description="Год выпуска от"),
    release_to: Optional[int] = Query(None, ge=1954, le=2024, description="Год выпуска до"),
    metacritic_from: Optional[float] = Query(None, ge=0, le=100, description="Metacritic от"),
    metacritic_to: Optional[float] = Query(None, ge=0, le=100, description="Metacritic до"),
    rating_from: Optional[float] = Query(None, ge=0, le=5, description="RAWG рейтинг от"),
    rating_to: Optional[float] = Query(None, ge=0, le=5, description="RAWG рейтинг до"),
    playtime_from: Optional[float] = Query(None, ge=0, le=2630, description="Игровое время от"),
    playtime_to: Optional[float] = Query(None, ge=0, le=2630, description="Игровое время до"),
    percent_recommended_from: Optional[float] = Query(None, ge=0, le=100, description="Процент рекомендаций от"),
    percent_recommended_to: Optional[float] = Query(None, ge=0, le=100, description="Процент рекомендаций до"),
    sort_by: Optional[str] = Query("popularity", description="Сортировка по: popularity, rating_all, released")
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

    add_filter("released", release_from, release_to)
    add_filter("metacritic", metacritic_from, metacritic_to)
    add_filter("rating", rating_from, rating_to)
    add_filter("playtime", playtime_from, playtime_to)
    add_filter("percent_recommended", percent_recommended_from, percent_recommended_to)

    if genres:
        genre_list = genres.split(",")
        genre_placeholders = ", ".join(["%s"] * len(genre_list))
        filters.append(f"games.id IN (SELECT DISTINCT id_game FROM games_genres_link WHERE id_genre IN (SELECT id FROM games_genres WHERE genre IN ({genre_placeholders})))")
        params.extend(genre_list)

    if platforms:
        platform_list = platforms.split(",")
        platform_placeholders = ", ".join(["%s"] * len(platform_list))
        filters.append(f"games.id IN (SELECT DISTINCT id_game FROM games_platforms_link WHERE id_platform IN (SELECT id FROM games_platforms WHERE platform IN ({platform_placeholders})))")
        params.extend(platform_list)

    valid_sort_columns = ["popularity", "rating_all", "released"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail="Некорректное значение sort_by")

    query = f"""
        SELECT DISTINCT games.*
        FROM games
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


@app.get("/games/by_genre/")
def get_games_by_genre(
    genre: str,
    platforms: Optional[str] = None,  # Список платформ в виде строки, разделенной запятыми
    offset: int = 0,
    limit: int = 20,
    sort_by: str = "popularity"
):
    """
    Возвращает список игр по жанру и платформам с пагинацией и сортировкой.
    """
    valid_sort_columns = ["popularity", "rating_all", "released"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail=f"Некорректное значение sort_by. Возможные значения: {valid_sort_columns}")
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Базовый SQL-запрос (фильтр только по жанру)
        query = f"""
            SELECT DISTINCT g.id, g.name, g.poster_cloud, g.popularity, g.rating, g.rating_all, 
                            g.metacritic, g.released, g.genre, g.percent_recommended, g.platforms
            FROM games g
            JOIN games_genres_link ggl ON g.id = ggl.id_game
            JOIN games_genres gg ON ggl.id_genre = gg.id
        """

        params = [genre]

        # Если переданы платформы, добавляем фильтр
        if platforms:
            platform_list = platforms.split(",")  # Преобразуем в список строк
            placeholders = ', '.join(['%s'] * len(platform_list))  # Создаем плейсхолдеры для SQL
            query += f"""
                JOIN games_platforms_link gpl ON g.id = gpl.id_game
                JOIN games_platforms gp ON gpl.id_platform = gp.id
                WHERE gg.genre = %s
                AND gp.platform IN ({placeholders})
            """
            params.extend(platform_list)
        else:
            query += " WHERE gg.genre = %s"

        # Добавляем сортировку, лимит и оффсет
        query += f" ORDER BY g.{sort_by} DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        # Выполняем запрос
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        if not rows:
            raise HTTPException(status_code=404, detail="Игры не найдены для указанного жанра и платформ")

        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/games/by_platform/", response_model=List[dict])
def get_games_by_platform(
    platform: str = Query(..., description="Название платформы"),
    offset: int = Query(0, ge=0, description="Смещение (пагинация)"),
    limit: int = Query(20, ge=1, le=100, description="Количество записей на странице"),
    sort_by: str = Query("popularity", description="Сортировка по: popularity, rating_all, released")
):
    """
    Возвращает список игр для указанной платформы с поддержкой пагинации и сортировки.
    """
    valid_sort_columns = ["popularity", "rating_all", "released"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail=f"Некорректное значение sort_by. Доступные значения: {valid_sort_columns}")

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = f"""
            SELECT DISTINCT g.id, g.name, g.poster_cloud, g.popularity, g.rating,g.rating_all, g.metacritic, g.released, g.genre , g.percent_recommended
            FROM games g
            JOIN games_platforms_link gpl ON g.id = gpl.id_game
            JOIN games_platforms gp ON gpl.id_platform = gp.id
            WHERE gp.platform = %s
            ORDER BY g.{sort_by} DESC
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, (platform, limit, offset))
        rows = cursor.fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="Игры не найдены для указанной платформы")

        return rows
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/games/by_tag/", response_model=List[dict])
def get_games_by_tag(
    tag: str = Query(..., description="Название тега"),
    offset: int = Query(0, ge=0, description="Смещение (пагинация)"),
    limit: int = Query(20, ge=1, le=100, description="Количество записей на странице"),
    sort_by: str = Query("popularity", description="Сортировка по: popularity, rating_all, released")
):
    """
    Возвращает список игр для указанного тега с поддержкой пагинации и сортировки.
    """
    valid_sort_columns = ["popularity", "rating_all", "released"]
    if sort_by not in valid_sort_columns:
        raise HTTPException(status_code=400, detail=f"Некорректное значение sort_by. Доступные значения: {valid_sort_columns}")

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = f"""
            SELECT DISTINCT g.id, g.name,g.id_rawg, g.poster_cloud, g.popularity, g.rating, g.rating_all,g.metacritic, g.released,  g.genre , g.percent_recommended
            FROM games g
            JOIN games_tag gt ON g.id_rawg = gt.id
            WHERE gt.tag = %s
            ORDER BY g.{sort_by} DESC
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, (tag, limit, offset))
        rows = cursor.fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="Игры не найдены для указанного тега")

        return rows
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()