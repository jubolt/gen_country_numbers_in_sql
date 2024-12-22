import sqlite3
import multiprocessing

# Path to the database
db_path = r'C:\1.db'


operators = {
    'Kyivstar': ['067', '068', '096', '097', '098', '039', '077'],
    'Vodafone': ['050', '066', '095', '099', '075'],
    'Lifecell': ['063', '073', '093']
}

# Create a table for a specific code
def create_table(code):
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS numbers_{code} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT UNIQUE,
            operator TEXT,
            identifikator INTEGER,
            in_viber BOOLEAN,
            day_of_check TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# Function for generating and inserting numbers
def generate_and_insert(code, operator, start, end):
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    cursor = conn.cursor()
    batch = []
    
    for i in range(start, end):  # Generate numbers in a given range
        number = f"38{code}{i:07}"  # Full number format
        batch.append((number, operator, 0, False, 0))  # identifikator = 0
        if len(batch) >= 60000:  # Вставляем партии по 60 000
            cursor.executemany(f'''
                INSERT OR IGNORE INTO numbers_{code} (number, operator, identifikator, in_viber, day_of_check)
                VALUES (?, ?, ?, ?, ?)
            ''', batch)
            conn.commit()
            batch = []
    
    if batch:  # Insert the remaining numbers
        cursor.executemany(f'''
            INSERT OR IGNORE INTO numbers_{code} (number, operator, identifikator, in_viber, day_of_check)
            VALUES (?, ?, ?, ?, ?)
        ''', batch)
        conn.commit()
    
    conn.close()

# Parallel generation
def parallel_generation():
    #We determine the number of threads (by the number of processor cores)
    num_processes = multiprocessing.cpu_count()
    print(f"Используется {num_processes} потоков")
    
    # We split the range of numbers into parts for each operator code
    total_numbers = 10000000  # Total number of numbers to generate
    chunk_size = total_numbers // num_processes  # Part size for each thread
    
    tasks = []
    for operator, codes in operators.items():
        for code in codes:
            create_table(code)  # Создаем таблицу для текущего кода
            for i in range(num_processes):
                start = i * chunk_size
                end = (i + 1) * chunk_size if i != num_processes - 1 else total_numbers
                tasks.append((code, operator, start, end))
    
    # Parallel run of generation and insertion
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.starmap(generate_and_insert, tasks)

if __name__ == '__main__':
    parallel_generation()
