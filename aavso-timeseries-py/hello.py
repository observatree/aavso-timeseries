
import os

import mysql.connector

# noinspection PyUnresolvedReferences
cnx = mysql.connector.connect(user='leavitt', password=os.environ['MYSQL_PASSWORD'],
                              host='127.0.0.1',
                              database='test')

cnx.close()
