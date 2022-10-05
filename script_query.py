import logging
from datetime import date
import msvcrt as m
import os
import json
import psycopg2 as psg
import codecs
import pandas as pd
from pandas.io.excel import ExcelWriter


logging.basicConfig(format='%(asctime)s - %(message)s',
                    filename='script_log.log', encoding='utf-8', level=logging.DEBUG)
today = date.today()
dir_path = os.path.dirname(os.path.realpath(__file__))

try:

    """connection.json path"""
    connection_path = dir_path + "\connections.json"

    query1 = '\query1.sql'
    query2 = '\query2.sql'
    query3 = '\query3.sql'


    def wait():
        """Задержка экрана"""
        m.getch()


    def connect(db):
        logging.info('User: %s', db["USER"])
        try:
            connect_to = psg.connect(database=db["BASE"], user=db["USER"],
                                     password=db["PASSWORD"], host=db["HOST"],
                                     port=db["PORT"])
            logging.info('Connection to %s is success', db["BASE"])
            return connect_to
        except Exception as error:
            logging.warning('Connection to %s failed: %s', db["BASE"], error)


    def read_query_from_file(file_name):
        sql_path = dir_path + f'\sql_requests{file_name}'
        try:
            with codecs.open(sql_path, 'r', "utf_8_sig") as q:
                que = q.read()
            logging.info('Query %s read successfully', file_name)
            return que
        except Exception as error:
            logging.warning('Query %s read ERROR: %s', file_name, error)


    def send_query(que_sql, conn):
        try:
            curr = conn.cursor()
            logging.info('Send query to DB')
            curr.execute(que_sql)
            response = curr.fetchall()
            return response
        except Exception as error:
            logging.warning('Send query to DB is failed: %s', error)


    if __name__ == '__main__':
        with open(connection_path, 'r') as f:
            dict_db = json.load(f)
        excel_path = dir_path + f'\sql_response\Отчёт_{today}.xlsx'
        with ExcelWriter(excel_path, engine="openpyxl",
                         mode="a" if os.path.exists(excel_path) else "w") as writer:
            try:
                connection_rem_bso = connect(dict_db["db1"])
                query = read_query_from_file(query1)
                response_rem_bso = send_query(query, connection_rem_bso)
                df = pd.DataFrame(response_rem_bso)
                df.to_excel(writer, sheet_name='sheet1', index=False)
                logging.info('result has been written')
                connection_rem_bso.close()
            except Exception as e:
                logging.warning('%s', {e})
            try:
                connection_rem_lim = connect(dict_db["REM_LIM"])
                query = read_query_from_file(query2)
                response_rem_lim = send_query(query, connection_rem_lim)
                df = pd.DataFrame(response_rem_lim)
                df.to_excel(writer, sheet_name='sheet2', index=False)
                logging.info('result has been written')
                connection_rem_lim.close()
            except Exception as e:
                logging.warning('%s', {e})
            try:
                connection_use_lim = connect(dict_db["USE_LIM"])
                query = read_query_from_file(query3)
                response_use_lim = send_query(query, connection_use_lim)
                df = pd.DataFrame(response_use_lim)
                df.to_excel(writer, sheet_name='Sheet3', index=False)
                logging.info('result has been written')
                connection_use_lim.close()
            except Exception as e:
                logging.warning(' | %s', {e})
            writer.save()
except Exception as e:
    logging.warning('%s', {e})


wait()
