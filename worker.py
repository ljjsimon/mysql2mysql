import os
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from binlog2sql_util import concat_sql_from_binlog_event

mysql_setting = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": "yanchupiaowu",
    "charset": "utf8mb4"
}
remote_mysql_setting = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": "yanchupiaowu",
    "charset": "utf8mb4",
    "db": "ljj_remote"
}
only_schemas = ['ljj']
only_tables = ['articles1']
binlog_file = 'binlog.000003'
log_pos=4

def write_log(filename, str):
    f = open(filename, 'w')
    f.write(str)
    f.close()

def main():
    global binlog_file, log_pos
    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]
    log_file='position.log'
    
    if(os.path.exists(log_file)):
        file = open(log_file,'r')
        logstr = file.read()
        arr = logstr.split('|')
        binlog_file=arr[0]
        log_pos=int(arr[1])
    
    local_conn = pymysql.connect(**mysql_setting)
    remote_conn = pymysql.connect(**remote_mysql_setting)
    stream = BinLogStreamReader(connection_settings=mysql_setting, server_id=100,
                                log_file=binlog_file, log_pos=log_pos, only_events=only_events,
                                only_schemas=only_schemas, only_tables=only_tables, resume_stream=True)

    with local_conn as cursor, remote_conn as remote_cursor:
        for binlog_event in stream:
            for row in binlog_event.rows:
                sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event,row=row)
                print(sql)
                #remote_cursor.execute(sql)
                log=stream.log_file+'|'+str(binlog_event.packet.log_pos)
                write_log(log_file, log)

    stream.close()
    local_conn.close()
    remote_conn.close()

if __name__ == "__main__":
    main()