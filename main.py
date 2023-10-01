import sys
import psycopg2
from datetime import datetime
import random
import string
import time

def connection():
    db_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'test',
    'user': 'postgres',
    'password': 'Telega_13'
    }

    # Подключение к базе данных
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    return conn, cur


def creadte_table():
    conn, cur = connection()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            full_name VARCHAR(100),
            birth_date DATE,
            gender VARCHAR(10)
        )
    """)
    conn.commit()
    print("Таблица создана успешно!")

class Employee:
    def __init__(self, full_name, birth_date, gender):
        self.full_name = full_name
        self.birth_date = birth_date
        self.gender = gender

    def calculate_age(self):
        today = datetime.today()
        age = today.year - self.birth_date.year
        if today.month < self.birth_date.month or (today.month == self.birth_date.month and today.day < self.birth_date.day):
            age -= 1
        return age

    def save_to_database(self):
        conn, cur = connection()
        cur.execute("INSERT INTO employees (full_name, birth_date, gender) VALUES (%s, %s, %s)",
                        (self.full_name, self.birth_date, self.gender))
        conn.commit()
        print("Запись успешно добавлена в базу данных")

def show_employees():
    conn, cur = connection()
    cur.execute("""
        SELECT full_name, birth_date, gender
        FROM employees
        ORDER BY full_name
    """)
    results = cur.fetchall()
    print('База данных:')
    for row in results:
        age = Employee('', row[1], '').calculate_age()
        print(f'ФИО: {row[0]}, Дата рождения: {row[1]}, пол: {row[2]}, возраст:{age}')
    cur.close()
    conn.close()

def generate_random_employee():
    full_name = ' '.join([''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=5)) for _ in range(3)])
    birth_date = datetime.strptime(f"{random.randint(1950, 2000)}-{random.randint(1, 12)}-{random.randint(1, 28)}", '%Y-%m-%d')
    gender = random.choice(['Male', 'Female'])
    return Employee(full_name, birth_date, gender)

def fill_employees(n):
    for _ in range(n):
        employee = generate_random_employee()
        employee.save_to_database()

def fill_special_employees(n):
    for _ in range(n):
        full_name = 'F' + ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=4)) + ' ' + \
                    ' '.join([''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=5)) for _ in range(2)])
        birth_date = datetime.strptime(f"{random.randint(1950, 2000)}-{random.randint(1, 12)}-{random.randint(1, 28)}", '%Y-%m-%d')
        gender = 'Male'
        employee = Employee(full_name, birth_date, gender)
        employee.save_to_database()

def select_special_employees():
    start_time = time.time()
    conn, cur = connection()
    
    cur.execute("""
        SELECT full_name, birth_date, gender, age(birth_date) AS age
        FROM employees
        WHERE gender = 'Male' AND full_name LIKE 'F%'
    """)
    results = cur.fetchall()
    
    end_time = time.time()
    execution_time = end_time - start_time
    for row in results:
        print(row)
    print(f'Время выполнения: {round(execution_time, 10)} секунд')

def optimize_db():
    conn, cur = connection()
    cur.execute("CREATE INDEX idx_gender ON employees (gender)")
    cur.execute("CREATE INDEX idx_full_name ON employees (full_name text_pattern_ops)")
    conn.commit()
    print("База данных успешно оптимизирована")



def main():
    if len(sys.argv) != 2:
        print("Необходимо указать режим работы приложения")
        sys.exit(1)
    else:
        param = sys.argv[1]
        if param == "1":
            creadte_table()
        elif param == "2":
            if len(sys.argv) < 5:
                print("Необходимо указать ФИО, дату рождения и пол")
                sys.exit(1)
            else:
                full_name = sys.argv[2]
                birth_date = datetime.strptime(sys.argv[3], "%Y-%m-%d").date()
                gender = sys.argv[4]
                add_to_bd = Employee(full_name, birth_date, gender)
                add_to_bd.save_to_database()
        elif param == "3":
            show_employees()
        elif param == "4":
            fill_employees(1000000)
            fill_special_employees(100)
        elif param == "5":
            select_special_employees()
        elif param == "6":
            optimize_db()
            select_special_employees()
        else:
            print("Неправильный параметр. Используйте от 1 до 6")

if __name__ == "__main__":
    main()
