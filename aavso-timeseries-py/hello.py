import os

# The following import will bomb out unless you have done something like
# $ conda install -c anaconda mysql-connector-python=2.0.4
# in your anaconda3 environment. Currently, my environment is
# ~/Applications/Anaconda/anaconda3/envs/py361/bin/python
import mysql.connector
import time

from processing import identify_timeseries, Observation, N0, N1, N2, N3, N4, TIMESERIES_THRESHOLD

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
    # noinspection PyUnresolvedReferences
    cnt = 0
    cnx_outer = mysql.connector.connect(user=USER,
                                        password=PASSWORD,
                                        host=HOST,
                                        database=DATABASE)  # type: mysql.connector.connection.MySQLConnection
    outer_cursor = cnx_outer.cursor(buffered=True)  # buffer so its not necessary to read the whole result
    outer_query = "SELECT DISTINCT name, obscode, band FROM temp_observations"
    outer_cursor.execute(outer_query)
    print(outer_cursor.rowcount)  # how many cases to process?
    for (star_name, observer_code, band) in outer_cursor:
        # print("Processing {}'s observations of {} in band {}....".format(observer_code, star_name, band))
        observations = read_name_observer_band(star_name, observer_code, band)
        timeseries_dict = identify_timeseries(observations)
        # print("Identified {} time series".format(len(timeseries_dict)))
        write_timeseries(timeseries_dict)
        cnt += 1
        if 0 == (cnt % 100):  # A progress-bar, because it's going to take a while
            print(cnt, N0, N1, N2, N3, N4)

    outer_cursor.close()
    cnx_outer.close()

    print(N0, N1, N2, N3, N4)


def processing_loop2():
    # concept: single pass through the db. Maintain a table for each name+obscode combinations with the
    #   the last obs. Write the timeseries determination for the last observation seen; put the current
    #   one into the table. And finally write everything left in the table to the db.
    start_time = time.time()
    cnx_outer = mysql.connector.connect(user=USER,
                                        password=PASSWORD,
                                        host=HOST,
                                        database=DATABASE)  # type: mysql.connector.connection.MySQLConnection
    outer_cursor = cnx_outer.cursor(buffered=True)  # buffer so its not necessary to read the whole result
    outer_query = "SELECT unique_id, JD, name, obscode FROM temp_observations order by JD"
    outer_cursor.execute(outer_query)
    print(outer_cursor.rowcount, ' %d sec' % (time.time() - start_time))  # how many cases to process?
    cnt = 0

    lastobs = dict()
    # lastobsentry = collections.namedtuple("lastobsentry", "unique_id jd ts")
    ind_id = 0
    ind_jd = 1
    ind_ts = 2
    # set up to write the ts field
    cnx = mysql.connector.connect(user=USER,
                                  password=PASSWORD,
                                  host=HOST,
                                  database=DATABASE)  # type: mysql.connector.connection.MySQLConnection
    write_cursor = cnx.cursor()

    for (unique_id, JD, name, obscode) in outer_cursor:
        jdf = float(JD)  # nb JD is varchar
        h = name + obscode  # simplest hash
        if h in lastobs:
            if jdf - lastobs[h][ind_jd] < TIMESERIES_THRESHOLD:  # this obs is in a ts
                if None is lastobs[h][ind_ts]:  # This is the start of the series
                    lastobs[h][ind_ts] = lastobs[h][ind_id]  # ts is the id of the first obs
                clearts = False
            else:
                clearts = True
            # if we knew db had nulls in timeseries, could avoid writing the nulls over again
            update_statement = ('UPDATE temp_observations SET timeseries2={} '
                                'WHERE unique_id={}').format(lastobs[h][ind_ts], lastobs[h][ind_id])
            # print(update_statement)
            if lastobs[h][ind_ts]:
                write_cursor.execute(update_statement)
            #   this fails when trying to write None to the timeseries2 field. Needs to be null
            #   this next line solves that problem, supposedly
            #   but nothing is getting written.
            #   update_statement = """UPDATE temp_observations SET timeseries2=%s WHERE unique_id=%s"""
            #   write_cursor.execute(update_statement, lastobs[h][ind_ts], lastobs[h][ind_id])

            lastobs[h][ind_id] = unique_id
            lastobs[h][ind_jd] = jdf
            if clearts:
                lastobs[h][ind_ts] = None
            # else leave the timeseries starting id in place

        else:
            lastobs[h] = [unique_id, jdf, None]
        cnt += 1
        if 0 == (cnt % 100000):
            print(cnt, lastobs.__len__(), ' %d sec' % (time.time() - start_time))
            cnx.commit()

    print(cnt, lastobs.__len__(), ' %d sec' % (time.time() - start_time))
    # clear the lastobs to the file
    #    this doesn't work....
    # for lo in lastobs:
    #     update_statement = ('UPDATE temp_observations SET timeseries2={} '
    #                         'WHERE unique_id={}').format(lo[ind_ts], lo[ind_id])
    #     if (lo[ind_ts] != None):
    #         write_cursor.execute(update_statement)

    # wrap up write
    write_cursor.close()
    cnx.commit()
    cnx.close()
    # wrap up outer
    outer_cursor.close()
    cnx_outer.close()


if __name__ == "__main__":
    outer_processing_loop()
    # processing_loop2()
