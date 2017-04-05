
import os

import mysql.connector

from processing import identify_timeseries, Observation

USER = os.environ['MYSQL_USER']  # 'leavitt'
HOST = '127.0.0.1'
DATABASE = 'test'
# So that the password does not appear in the code, the password is read from an environment variable.


# noinspection PyUnresolvedReferences
cnx_outer = mysql.connector.connect(user=USER, password=os.environ['MYSQL_PASSWORD'],
                                    host=HOST,
                                    database=DATABASE)   # type: mysql.connector.connection.MySQLConnection

outer_cursor = cnx_outer.cursor()

outer_query = "SELECT DISTINCT name, obscode FROM temp_observations"

outer_cursor.execute(outer_query)

# noinspection PyUnresolvedReferences
cnx_inner = mysql.connector.connect(user=USER, password=os.environ['MYSQL_PASSWORD'],
                                    host=HOST,
                                    database=DATABASE)   # type: mysql.connector.connection.MySQLConnection

inner_cursor = cnx_inner.cursor()

for (name, observer_code) in outer_cursor:
    print("Processing {}'s observations of {}....".format(observer_code, name))
    inner_query = ('SELECT unique_id, JD FROM temp_observations '
                   'WHERE name="{}" AND obscode="{}"').format(name, observer_code)
    inner_cursor = cnx_inner.cursor()
    inner_cursor.execute(inner_query)
    observations = []
    for (unique_id, julian_date) in inner_cursor:
        observation = Observation(unique_id=unique_id,
                                  julian_date_string=julian_date,
                                  julian_date=None,
                                  timeseries=None)
        observations.append(observation)

    time_series = identify_timeseries(observations)


cnx_inner.close()

cnx_outer.close()
