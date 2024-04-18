#from faker import Faker
#import time
#import pymysql.cursors
#from pymongo import MongoClient
#
#mysql_conn = pymysql.connect(host='localhost',
#                             port=3306,
#                             user='root',
#                             password='password',
#                             db='data',
#                             charset='utf8mb4',
#                             cursorclass=pymysql.cursors.DictCursor)
#
#mongo_client = MongoClient('localhost', 27017)
#mongo_db = mongo_client['database']
#mongo_collection = mongo_db['collection']
#
#
#def generate_fake_data():
#    fake = Faker()
#    data = []
#    for _ in range(1000000):
#        record = {
#            'name': fake.name(),
#            'address': fake.address(),
#            'phone_number': fake.phone_number()
#        }
#        data.append(record)
#    return data
#
#
#def insert_into_mysql(data):
#    with mysql_conn.cursor() as cursor:
#        sql = "INSERT INTO datatable (name, address, phone_number) VALUES (%s, %s, %s)"
#        for record in data:
#            cursor.execute(sql, (record['name'], record['address'], record['phone_number']))
#    mysql_conn.commit()
#
#
#def insert_into_mongodb(data):
#    mongo_collection.insert_many(data)
#
#
#def measure_time(func, *args, **kwargs):
#    start_time = time.time()
#    func(*args, **kwargs)
#    end_time = time.time()
#    return end_time - start_time
#
#
#if __name__ == "__main__":
#    data = generate_fake_data()
#
#    mysql_insert_time = measure_time(insert_into_mysql, data)
#    print(f"Data inserted into MySQL database in {mysql_insert_time} seconds.")
#
#    mongo_insert_time = measure_time(insert_into_mongodb, data)
#    print(f"Data inserted into MongoDB database in {mongo_insert_time} seconds.")
#
#    with mysql_conn.cursor() as cursor:
#        cursor.execute("CREATE INDEX idx_name ON datatable (name)")
#        cursor.execute("CREATE INDEX idx_address ON datatable (address)")
#    mysql_conn.commit()
#    print("Indexes created in MySQL database.")
#
#    mongo_collection.create_index([('name', 1)])
#    mongo_collection.create_index([('address', 1)])
#    print("Indexes created in MongoDB collection.")
#
#    query = {'name': 'John Doe'}
#
#    mysql_query_time = measure_time(
#        lambda: cursor.execute("SELECT * FROM datatable WHERE name = %s", (query['name'],)))
#    print(f"MySQL query with index executed in {mysql_query_time} seconds.")
#
#    mongo_query_time = measure_time(lambda: mongo_collection.find(query))
#    print(f"MongoDB query with index executed in {mongo_query_time} seconds.")
#
#    mysql_conn.close()
#    mongo_client.close()

from faker import Faker
import time
import json
import pymysql.cursors
from pymongo import MongoClient

mysql_conn = pymysql.connect(host='localhost',
                             port=3306,
                             user='root',
                             password='password',
                             db='data',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

mongo_client = MongoClient('localhost', 27017, username='root', password='password')
mongo_db = mongo_client['database']
mongo_collection = mongo_db['collection']


def generate_fake_data():
    fake = Faker()
    data = []
    for i in range(1000000):
        record = {
            'name': fake.name(),
            'address': fake.address(),
            'phone_number': fake.phone_number()
        }
        data.append(record)
        if (i + 1) % 10000 == 0:
            print(f"{i + 1} records generated.")
    return data


def write_to_file(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file)
    print(f"Data written to {filename}.")


def insert_into_mysql_from_file(filename):
    print("Inserting datasets into MySQL from file...")
    with open(filename, 'r') as file:
        data = json.load(file)
        with mysql_conn.cursor() as cursor:
            sql = "INSERT INTO datatable (name, address, phone_number) VALUES (%s, %s, %s)"
            for record in data:
                cursor.execute(sql, (record['name'], record['address'], record['phone_number']))
        mysql_conn.commit()
    print("Finished inserting datasets into MySQL.")


def insert_into_mongodb_from_file(filename):
    print("Inserting datasets into MongoDB from file...")
    with open(filename, 'r') as file:
        data = json.load(file)
        mongo_collection.insert_many(data)
    print("Finished inserting datasets into MongoDB.")


def measure_time(func, *args, **kwargs):
    start_time = time.time()
    func(*args, **kwargs)
    end_time = time.time()
    return end_time - start_time


if __name__ == "__main__":
    data = generate_fake_data()

    write_to_file(data, 'fake_data.json')

    mysql_filename = 'mysql_data.json'
    mongo_filename = 'mongo_data.json'

    mysql_data = [{'name': record['name'], 'address': record['address'], 'phone_number': record['phone_number']} for record in data]
    mongo_data = data

    write_to_file(mysql_data, mysql_filename)
    write_to_file(mongo_data, mongo_filename)

    mysql_insert_time = measure_time(insert_into_mysql_from_file, mysql_filename)
    print(f"Data inserted into MySQL database in {mysql_insert_time} seconds.")

    mongo_insert_time = measure_time(insert_into_mongodb_from_file, mongo_filename)
    print(f"Data inserted into MongoDB database in {mongo_insert_time} seconds.")

    with mysql_conn.cursor() as cursor:
        try:
            cursor.execute("DROP INDEX idx_name ON datatable")
        except pymysql.err.InternalError as e:
            if e.args[0] == 1091:
                pass
            else:
                raise

        cursor.execute("CREATE INDEX idx_name ON datatable (name)")

    mysql_conn.commit()
    print("Indexes created in MySQL database.")

    mongo_collection.create_index([('name', 1)])
    mongo_collection.create_index([('address', 1)])
    print("Indexes created in MongoDB collection.")

    query = {'name': 'John Doe'}

    with mysql_conn.cursor() as cursor:
        mysql_query_time = measure_time(
            lambda: cursor.execute("SELECT * FROM datatable WHERE name = %s", (query['name'],)))
        print(f"MySQL query with index executed in {mysql_query_time} seconds.")

    mongo_query_time = measure_time(lambda: mongo_collection.find(query))
    print(f"MongoDB query with index executed in {mongo_query_time} seconds.")

    mysql_conn.close()
    mongo_client.close()

