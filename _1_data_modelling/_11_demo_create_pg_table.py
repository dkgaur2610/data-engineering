"""
@author  Deepak Gaur
Python wrappers to create postgres tables
This tutorial is about the need of autocommit=True while creating a connection to avoid transaction related errors
"""

import psycopg2

try:
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
except psycopg2.Error as e:
    print("Connection unsuccessful - Ensure connection details are correct and server is running")
    print(e)

""" 
To avoid calling conn.commit() after every query we issue, lets set autocommit to True so every connection is 
committed
"""
conn.set_session(autocommit=True)

try:
    cur = conn.cursor()  # Cursor is used to execute query
except psycopg2.Error as e:
    print('Error getting cursor from the connection')
    print(e)

cur.execute("select datname from pg_database")
print(cur.fetchall())  # Returns a tuple with all the records inside the query
try:
    cur.execute("create table if not exists nanodemo (id int, name varchar(10))")
    cur.execute("select count(*) from nanodemo")
    print(cur.fetchall())
    cur.execute("insert into nanodemo values (4,'four'),(5,'five'),(6,'six')")
    cur.execute("select * from nanodemo")
    print(cur.fetchall())
except Exception as e:
    print(e)

# Lets create a new connection, create a new database and connect to it to get a cursor
"""
try:

except psycopg2.Error as e:
    print(e)
"""

try:
    # Since there is no such thing as 'if not exist' for database, thus we will need to do manual checks for it
    cur.execute("select datname from pg_database")
    dats = cur.fetchall()
    is_db_exist = False
    for dat in dats:
        print(dat[0])
        if dat[0] == 'udacity_demo':
            is_db_exist = True
    if not is_db_exist:
        cur.execute("CREATE DATABASE udacity_demo")
except psycopg2.Error as e:
    print(e)

try:
    conn.close()
except psycopg2.Error as e:
    print(e)

try:
    conn = psycopg2.connect("dbname=udacity_demo")
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    cur.execute("CREATE TABLE IF NOT EXISTS music_library (album_name varchar, artist_name varchar, year int)")
    cur.execute("SELECT count(*) FROM music_library")
    print('Number of records in current table : ')
    print(cur.fetchall())
    print('Inserting new records in the table...')
    cur.execute('INSERT INTO music_library  VALUES (%s, %s, %s)', ('Let It Be', 'The Beatles', '1970'))
    cur.execute('INSERT INTO music_library VALUES (%s, %s, %s)', ('Rubber Soul', 'The Beatles', '1965'))
    print('All Records of the table : ')
    cur.execute("SELECT * FROM music_library")
    print(cur.fetchall())
except psycopg2.Error as e:
    print(e)

# We are done with creating a table and deleting records from it - lets drop it and close connections

try:
    print('Dropping the table...')
    cur.execute("DROP TABLE music_library")
    cur.close()
    conn.close()
except psycopg2.Error as e:
    print(e)