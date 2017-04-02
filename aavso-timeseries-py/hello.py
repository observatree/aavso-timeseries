
import os

import mysql.connector

from processing import identify_timeseries

# noinspection PyUnresolvedReferences
cnx_outer = mysql.connector.connect(user='leavitt', password=os.environ['MYSQL_PASSWORD'],
                                    host='127.0.0.1',
                                    database='test')   # type: mysql.connector.connection.MySQLConnection

outer_cursor = cnx_outer.cursor()

outer_query = "SELECT DISTINCT name, obscode FROM temp_observations"

outer_cursor.execute(outer_query)

# noinspection PyUnresolvedReferences
cnx_inner = mysql.connector.connect(user='leavitt', password=os.environ['MYSQL_PASSWORD'],
                                    host='127.0.0.1',
                                    database='test')   # type: mysql.connector.connection.MySQLConnection

inner_cursor = cnx_inner.cursor()

for (name, observer_code) in outer_cursor:
    inner_query = "SELECT * FROM temp_observations WHERE name=\"{}\" AND obscode=\"{}\"".format(name, observer_code)
    inner_cursor = cnx_inner.cursor()
    inner_cursor.execute(inner_query)
    records = []
    for record in inner_cursor:
        records.append(record)
        time_series = identify_timeseries(records)

    print("Processed {}'s observations of {}".format(observer_code, name))


cnx_inner.close()

cnx_outer.close()
