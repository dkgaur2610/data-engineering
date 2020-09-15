"""
Demo to create tables in the cassandra NoSQL database
@author : Deepak Gaur
@date   : 09/14/2020
"""

import cassandra
from cassandra.cluster import Cluster

# Create initial Cluster Connection. We will get an error if something is wrong
try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    # Lets create a keyspace to store our data (analogous to database in RDBMS)
    session.execute("""CREATE KEYSPACE IF NOT EXISTS udacity_demo WITH REPLICATION = 
                           {'class':'SimpleStrategy', 'replication_factor':1}
    """)
    # Unlike Postgres, Cassandra you can connect to a keyspace withour reconnecting
    session.set_keyspace('udacity_demo')

    # We will create a simple music library table in cassandra too. Here, we need to know our query before desiging the
    # table. We are for example, interested in knowing all albums made in 1970 : select * from music_albums where year = 1970
    # The table is supposed to contain the Song Title, Artist Name and Year. Since I want to search by Year, thus the partition key
    # will be Year. The clustering column will be artist name to make primary key unique. Note that cassandra can not
    # have duplicate columns
    """
    Table : music_library
    col1  : Album Name
    col2  : Artist Name
    col3  : Year
    Primary Key   :  (artist_name, year)
    """
    query = """ CREATE TABLE IF NOT EXISTS music_library
                (album_name text, artist_name text, year int, 
                PRIMARY KEY("year", "artist_name")) """
    session.execute(query)

    # Try to get count from the table - only for Demo. Not expected on Prod Database - might even throw an error
    query = "SELECT COUNT(*) FROM music_library"
    count = session.execute(query)
    print(count.one())

    # Insert two rows in cluster
    query = "insert into music_library (album_name, artist_name, year) values (%s, %s, %s)"
    session.execute(query, ('Let it Be','The Beatles', 1970))
    query = "insert into music_library(album_name, artist_name, year) values (%s, %s, %s)"
    session.execute(query, ('In The Air', 'The Chainsmokers', 1965))

    # Select count of rows now
    query = "SELECT * FROM music_library"
    count = session.execute(query)
    print(count.all())


    # Drop Table and shutdown sessions
    session.execute('drop table music_library')
    session.shutdown()
    cluster.shutdown()

except Exception as e:
    print(e)



