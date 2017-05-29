import os

# The following import will bomb out unless you have done something like
# $ conda install -c anaconda mysql-connector-python=2.0.4
# in your anaconda3 environment. Currently, my environment is
# ~/Applications/Anaconda/anaconda3/envs/py361/bin/python
import mysql.connector
import time

from timeseries_identification import identify_timeseries, Observation

USER = os.environ['MYSQL_USER']
# So that the password does not appear in the code, the password is read from an environment variable.
PASSWORD = os.environ['MYSQL_PASSWORD']
HOST = '127.0.0.1'
DATABASE = 'test'


def read_name_observer_band(name, observer, band):
    # noinspection PyUnresolvedReferences
    cnx_inner = mysql.connector.connect(user=USER,
                                        password=PASSWORD,
                                        host=HOST,
                                        database=DATABASE)  # type: mysql.connector.connection.MySQLConnection
    inner_query = ('SELECT unique_id, JD FROM temp_observations '
                   'WHERE name="{}" AND obscode="{}" AND band="{}"').format(name, observer, band)
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
    """Runs a query that identifies all the name/obscode/band combinations.

    For each combination, reads in all observations for that combination.

    Criteria for identifying a times series
    """
    # noinspection PyUnresolvedReferences
    cnt = 0  # For monitoring progress.
    start_time = time.time()
    cnx_outer = mysql.connector.connect(user=USER,
                                        password=PASSWORD,
                                        host=HOST,
                                        database=DATABASE)  # type: mysql.connector.connection.MySQLConnection
    outer_cursor = cnx_outer.cursor(buffered=True)  # buffer so it's not necessary to read the whole result
    outer_query = "SELECT DISTINCT name, obscode, band FROM temp_observations"
    outer_cursor.execute(outer_query)
    validation_dict = dict()
    for (star_name, observer_code, band) in outer_cursor:
        observations = read_name_observer_band(star_name, observer_code, band)
        timeseries_dict = identify_timeseries(observations, validation_dict)
        write_timeseries(timeseries_dict)
        cnt += 1
        if 0 == (cnt % 100):  # Indicate progress, because it's going to take a while.
            print("Processed star_name/observer_code/band combinations={}.", cnt)

    outer_cursor.close()
    cnx_outer.close()

    print("Processed {} combination, validation_dict={}".format(cnt, validation_dict))
    print('{} sec'.format(time.time() - start_time))


# I bracket script execution with this,

# mysql> ALTER TABLE temp_observations DISABLE KEYS;
# mysql> update temp_observations set timeseries=NULL;

# ... execute the script ...

# mysql> ALTER TABLE temp_observations ENABLE KEYS;

# but it still takes 1533 seconds (almost half an hour) on
# a 2.2 GHz Intel Core i7 MacBook Air.


if __name__ == "__main__":
    outer_processing_loop()
