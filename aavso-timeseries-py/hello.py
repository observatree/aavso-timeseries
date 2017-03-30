
import os

import mysql.connector

# noinspection PyUnresolvedReferences
cnx = mysql.connector.connect(user='leavitt', password=os.environ['MYSQL_PASSWORD'],
                              host='127.0.0.1',
                              database='test')   # type: mysql.connector.connection.MySQLConnection

cursor = cnx.cursor()

query = "SELECT DISTINCT obscode FROM temp_observations"

cursor.execute(query)

for (observer_code) in cursor:
    print("{}, is an obscode".format(observer_code))


cnx.close()

