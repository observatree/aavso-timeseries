
import os

import mysql.connector

# noinspection PyUnresolvedReferences
cnx = mysql.connector.connect(user='leavitt', password=os.environ['MYSQL_PASSWORD'],
                              host='127.0.0.1',
                              database='test')   # type: mysql.connector.connection.MySQLConnection

cursor = cnx.cursor()

query = "SELECT DISTINCT name, obscode FROM temp_observations"

cursor.execute(query)

for (name, observer_code) in cursor:
    print("Processing {}'s observations of {}".format(observer_code, name))

cnx.close()
