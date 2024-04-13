from faker import Faker
import time
import pymysql
from pymongo import MongoClient

mysql_conn = pymysql.connect(host='localhost',
                             user='user',
                             password='password',
                             db='database',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['database']
mongo_collection = mongo_db['collection']


def generate_fake_data():
    fake = Faker()
    data = []
    for _ in range(1000000):
        record = {
            'name': fake.name(),
            'address': fake.address(),
            'phone_number': fake.phone_number()
        }
        data.append(record)
    return data


def insert_into_mysql(data):
    with mysql_conn.cursor() as cursor:
        sql = "INSERT INTO your_table_name (name, address, phone_number) VALUES (%s, %s, %s)"
        for record in data:
            cursor.execute(sql, (record['name'], record['address'], record['phone_number']))
    mysql_conn.commit()


def insert_into_mongodb(data):
    mongo_collection.insert_many(data)


def measure_time(func, *args, **kwargs):
    start_time = time.time()
    func(*args, **kwargs)
    end_time = time.time()
    return end_time - start_time


if __name__ == "__main__":
    data = generate_fake_data()

    mysql_insert_time = measure_time(insert_into_mysql, data)
    print(f"Data inserted into MySQL database in {mysql_insert_time} seconds.")

    mongo_insert_time = measure_time(insert_into_mongodb, data)
    print(f"Data inserted into MongoDB database in {mongo_insert_time} seconds.")

    with mysql_conn.cursor() as cursor:
        cursor.execute("CREATE INDEX idx_name ON your_table_name (name)")
        cursor.execute("CREATE INDEX idx_address ON your_table_name (address)")
    mysql_conn.commit()
    print("Indexes created in MySQL database.")

    mongo_collection.create_index([('name', 1)])
    mongo_collection.create_index([('address', 1)])
    print("Indexes created in MongoDB collection.")

    query = {'name': 'John Doe'}

    mysql_query_time = measure_time(
        lambda: cursor.execute("SELECT * FROM your_table_name WHERE name = %s", (query['name'],)))
    print(f"MySQL query with index executed in {mysql_query_time} seconds.")

    mongo_query_time = measure_time(lambda: mongo_collection.find(query))
    print(f"MongoDB query with index executed in {mongo_query_time} seconds.")

    mysql_conn.close()
    mongo_client.close()
