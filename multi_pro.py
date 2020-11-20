import multiprocessing
import os
import sqlite3


def worker():
    pid = os.getpid()
    print("pid:", pid)
    conn = sqlite3.connect('testDB.db')
    cursor = conn.cursor()
    sql_create = "CREATE TABLE IF NOT EXISTS tableA ('as', bb)"
    cursor.execute(sql_create)
    sql_insert = "INSERT INTO tableA VALUES (?,?)"
    cursor.execute(sql_insert, ("pid" + str(pid), "+++"))
    conn.commit()
    conn.close()


if __name__ == '__main__':
    jobs = []
    for i in range(5):
        p = multiprocessing.Process(target=worker)
        jobs.append(p)
        p.start()
