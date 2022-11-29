import sys

import pymysql.cursors
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)

LOG_DB_HOST = 'localhost'
LOG_DB_NAME = 'real_estate_db'
SRC_DB_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASS = 'password'
TABLE = 'exchange_rates'

MYSQL_SETTINGS = {
    "host": 'localhost',
    "port": 3306,
    "user": 'root',
    "passwd": 'password'
}


def connect_log_db(host):
    return pymysql.connect(
        host=host,
        port=3306,
        user=MYSQL_USER,
        passwd=MYSQL_PASS,
        db=LOG_DB_NAME,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def last_file_pos(conlogdb):
    sql = ("SELECT log_file, log_pos FROM real_estate_changelog "
           "ORDER BY log_file DESC, log_pos DESC LIMIT 1")

    with conlogdb.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchone()


def master_status(conlogdb):
    sql = "SHOW MASTER STATUS"

    with conlogdb.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchone()


def insert_log_db(conlogdb, values):
    with conlogdb.cursor() as cursor:
        # Create a new record
        sql = (
            "REPLACE INTO `real_estate_changelog` "
            "(`db`, `tbl`, `created_at`, `log_file`, `log_pos`) "
            "VALUES (%s, %s, DATE_ADD(%s, INTERVAL - WEEKDAY(%s) DAY), %s, %s)")
        cursor.execute(sql, values)

    # connection is not autocommit by default. So you must commit to save
    # your changes.
    conlogdb.commit()


def main():
    values = None
    conlogdb = connect_log_db(LOG_DB_HOST)
    consrcdb = connect_log_db(SRC_DB_HOST)

    file_pos = last_file_pos(conlogdb)
    if file_pos is not None:
        log_file = file_pos['log_file']
        log_pos = file_pos['log_pos']
    else:
        file_pos = master_status(consrcdb)
        log_file = file_pos['File']
        log_pos = file_pos['Position']

    print("Starting streaming at file: %s, position: %s" % (log_file, log_pos))

    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS, resume_stream=True,
        server_id=123456789, log_file=log_file, log_pos=log_pos,
        only_events=[WriteRowsEvent, UpdateRowsEvent], blocking=True)
    #172313514
    # If previous week/table processed is the same, we avoid the INSERT as
    # its redundant and affects performance
    pweek = None
    ptable = None

    for binlogevent in stream:
        for row in binlogevent.rows:
            if binlogevent.table != TABLE:
                continue

            if isinstance(binlogevent, DeleteRowsEvent) or isinstance(binlogevent, WriteRowsEvent):
                values = row["values"]
            elif isinstance(binlogevent, UpdateRowsEvent):
                values = row["after_values"]
            else:
                continue

            if ptable == binlogevent.table and pweek == values['created_at'].strftime('%Y-%m-%d'):
                continue

            ptable = binlogevent.table
            pweek = values['created_at'].strftime('%Y-%m-%d')

            # action keys '0 unk, 1 ins, 2 upd, 3 del'
            event = (binlogevent.schema, binlogevent.table,
                     values['created_at'].strftime('%Y-%m-%d'),
                     values['created_at'].strftime('%Y-%m-%d'),
                     stream.log_file, int(stream.log_pos))
            insert_log_db(conlogdb, event)
            sys.stdout.flush()

    stream.close()
    print(' Process is done.')


if __name__ == "__main__":
    main()
