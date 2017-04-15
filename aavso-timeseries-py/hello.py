
import os


# The following import will bomb out unless you have done something like
# $ conda install -c anaconda mysql-connector-python=2.0.4
# in your anaconda3 environment. Currently, my environment is
# ~/Applications/Anaconda/anaconda3/envs/py361/bin/python
import mysql.connector

from processing import identify_timeseries, Observation, N0, N1, N2, N3, N4

USER = os.environ['MYSQL_USER']
# So that the password does not appear in the code, the password is read from an environment variable.
PASSWORD = os.environ['MYSQL_PASSWORD']
HOST = '127.0.0.1'
DATABASE = 'test'


def read_name_observer(name, observer):
    # noinspection PyUnresolvedReferences
    cnx_inner = mysql.connector.connect(user=USER,
                                        password=PASSWORD,
                                        host=HOST,
                                        database=DATABASE)  # type: mysql.connector.connection.MySQLConnection
    inner_query = ('SELECT unique_id, JD FROM temp_observations '
                   'WHERE name="{}" AND obscode="{}"').format(name, observer)
    inner_cursor = cnx_inner.cursor()
    inner_cursor.execute(inner_query)
    observations = []
    for (unique_id, julian_date) in inner_cursor:
        observation = Observation(unique_id=unique_id,
                                  julian_date_string=julian_date,
                                  julian_date=None)
        observations.append(observation)

    inner_cursor.close()
    cnx_inner.close()

    return observations


def write_timeseries(timeseries_dict):
    # noinspection PyUnresolvedReferences
    cnx = mysql.connector.connect(user=USER,
                                  password=PASSWORD,
                                  host=HOST,
                                  database=DATABASE)  # type: mysql.connector.connection.MySQLConnection
    write_cursor = cnx.cursor()
    for timeseries, observations in timeseries_dict.items():
        for observation in observations:
            update_statement = ('UPDATE temp_observations SET timeseries={} '
                                'WHERE unique_id={}').format(timeseries, observation.unique_id)
            write_cursor.execute(update_statement)
    write_cursor.close()
    cnx.commit()
    cnx.close()


def outer_processing_loop():
    # noinspection PyUnresolvedReferences
    cnt= 0
    cnx_outer = mysql.connector.connect(user=USER,
                                        password=PASSWORD,
                                        host=HOST,
                                        database=DATABASE)  # type: mysql.connector.connection.MySQLConnection
    outer_cursor = cnx_outer.cursor(buffered=True) # buffer so its not necessary to read the whole result
    outer_query = "SELECT DISTINCT name, obscode FROM temp_observations"
    outer_cursor.execute(outer_query)
    print(outer_cursor.rowcount) # how many cases to process?
    for (star_name, observer_code) in outer_cursor:
        #print("Processing {}'s observations of {}....".format(observer_code, star_name))
        observations = read_name_observer(star_name, observer_code)
        timeseries_dict = identify_timeseries(observations)
        #print("Identified {} time series".format(len(timeseries_dict)))
        write_timeseries(timeseries_dict)
        cnt+= 1
        if (cnt % 100) == 0: # A progress-bar, because it's going to take a while
            print (cnt, N0,N1,N2,N3,N4 )

    outer_cursor.close()
    cnx_outer.close()

    print (N0,N1,N2,N3,N4)


if __name__ == "__main__":
    outer_processing_loop()
