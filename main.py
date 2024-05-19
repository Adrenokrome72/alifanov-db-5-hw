import psycopg2
from psycopg2 import sql, Error

# Создание таблицы
def create_tables():
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(database="clients_db", user="postgres", password="postgrespassadm")

        # Создание курсора
        cursor = conn.cursor()

        # SQL запрос для создания таблицы

        create_table_query = '''
                    CREATE TABLE clients (
                        id serial PRIMARY KEY,
                        first_name VARCHAR (50),
                        last_name VARCHAR (50),
                        email VARCHAR (255) UNIQUE NOT NULL
                    );
                '''

        create_phones_table_query = '''
                    CREATE TABLE phones (
                        id serial PRIMARY KEY,
                        client_id INTEGER REFERENCES clients(id),
                        phone VARCHAR (20) UNIQUE NOT NULL
                    );
                '''
        # Выполнение SQL запроса
        cursor.execute(create_table_query)
        cursor.execute(create_phones_table_query)
        conn.commit()
        print("Таблицы успешно созданы")

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при подключении к PostgreSQL", error)

    finally:
        # Закрытие курсора и соединения
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")

# Добавление клиента со всеми данными
def insert_client(first_name, last_name, email):
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(database="clients_db", user="postgres", password="postgrespassadm")

        # Создание курсора
        cursor = conn.cursor()

        # SQL запрос для вставки данных
        insert_query = '''
            INSERT INTO clients (first_name, last_name, email)
            VALUES (%s, %s, %s);
        '''

        # Выполнение SQL запроса
        cursor.execute(insert_query, (first_name, last_name, email))
        conn.commit()
        print("Данные успешно вставлены в PostgreSQL")

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при вставке данных в PostgreSQL", error)

    finally:
        # Закрытие курсора и соединения
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")

#Добавление нового номера телефона
def add_phone_for_client(client_id, new_phone):
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(database="clients_db", user="postgres", password="postgrespassadm")

        # Создание курсора
        cursor = conn.cursor()

        # SQL запрос для добавления телефона
        add_phone_query = '''
                    INSERT INTO phones (client_id, phone)
                    VALUES (%s, %s);
                '''

        # Выполнение SQL запроса
        cursor.execute(add_phone_query, (client_id, new_phone))
        conn.commit()
        print("Новый телефон успешно добавлен для клиента в PostgreSQL")

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при добавлении телефона для клиента в PostgreSQL", error)

    finally:
        # Закрытие курсора и соединения
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")

# Обновление информации о клиенте
def update_client_data(client_id, first_name=None, last_name=None, email=None):
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(database="clients_db", user="postgres", password="postgrespassadm")

        # Создание курсора
        cursor = conn.cursor()

        # SQL запрос для обновления данных
        update_query = sql.SQL('''
                    UPDATE clients
                    SET first_name = COALESCE(%s, first_name),
                        last_name = COALESCE(%s, last_name),
                        email = COALESCE(%s, email)
                    WHERE id = %s;
                ''')

        # Выполнение SQL запроса
        cursor.execute(update_query, (first_name, last_name, email, client_id))
        conn.commit()
        print("Данные клиента успешно обновлены в PostgreSQL")

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при обновлении данных клиента в PostgreSQL", error)

    finally:
        # Закрытие курсора и соединения
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")

# Удаление номера телефона клиента
def delete_phone_for_client(phone_id, phone=None):
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(database="clients_db", user="postgres", password="postgrespassadm")

        # Создание курсора
        cursor = conn.cursor()

        # SQL запрос для удаления телефона
        delete_phone_query = '''
                    DELETE FROM phones WHERE client_id = %s AND phone = %s;
                '''

        # Выполнение SQL запроса
        cursor.execute(delete_phone_query, (phone_id, phone))
        conn.commit()
        print("Телефон успешно удален для клиента в PostgreSQL")

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при удалении телефона для клиента в PostgreSQL", error)

    finally:
        # Закрытие курсора и соединения
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")

# Удаление клиента
def delete_client(client_id):
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(database="clients_db", user="postgres", password="postgrespassadm")

        # Создание курсора
        cursor = conn.cursor()

        # SQL запрос для удаления клиента
        delete_client_query = '''
            DELETE FROM clients
            WHERE id = %s;
        '''

        # Выполнение SQL запроса
        cursor.execute(delete_client_query, (client_id,))
        conn.commit()
        print("Клиент успешно удален из PostgreSQL")

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при удалении клиента из PostgreSQL", error)

    finally:
        # Закрытие курсора и соединения
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")

# Поиск информации о клиенте
def find_client(first_name=None, last_name=None, email=None, phone=None):
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(database="clients_db", user="postgres", password="postgrespassadm")

        # Создание курсора
        cursor = conn.cursor()

        # SQL запрос для поиска клиента
        query = sql.SQL('''
                    SELECT c.id, c.first_name, c.last_name, c.email, array_agg(p.phone) AS phones
                    FROM clients c
                    LEFT JOIN phones p ON c.id = p.client_id
                    WHERE (c.first_name = %s OR %s IS NULL)
                      AND (c.last_name = %s OR %s IS NULL)
                      AND (c.email = %s OR %s IS NULL)
                      AND (p.phone = %s OR %s IS NULL)
                    GROUP BY c.id;
                ''')

        # Выполнение SQL запроса
        cursor.execute(query, (first_name, first_name, last_name, last_name, email, email, phone, phone))
        results = cursor.fetchall()

        if results:
            print("Найденный клиент:")
            for result in results:
                print(f"ID: {result[0]}")
                print(f"Имя: {result[1]}")
                print(f"Фамилия: {result[2]}")
                print(f"Email: {result[3]}")
                if result[4] == [None]:
                    print(f"Номер телефона отутствует")
                else:
                    print(f"Телефоны: {result[4]}")
        else:
            print("Клиент не найден")

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при поиске клиента в PostgreSQL", error)

    finally:
        # Закрытие курсора и соединения
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")

# Блок кода для проверки работоспособности

if __name__ == "__main__":
# create_tables()
# insert_client("Сергей", "Иванов", "ivanov@example.ru")
# add_phone_for_client(1, "1234567892")
# update_client_data(1, first_name="Сергей", last_name="Алифанов", email="alifanov@example.ru")
# delete_phone_for_client(1, "1234567891")
# delete_client(2)
 find_client(first_name="Сергей")