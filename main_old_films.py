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
