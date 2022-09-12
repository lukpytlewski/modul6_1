import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def execute_sql(conn, sql):
    """Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_szczyt(conn, szczyt):
    """
    Create a new szczyt into the szczyty table
    :param conn:
    :param szczyt:
    :return: szczyt id
    """
    sql = """INSERT INTO szczyty(nazwa, wysokosc_bezwzgledna, wybitnosc)
             VALUES(?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, szczyt)
    conn.commit()
    return cur.lastrowid


def add_wyprawa(conn, wyprawa):
    """
    Create a new wyprawa into the wyprawy table
    :param conn:
    :param wyprawa:
    :return: wyprawa id
    """
    sql = """INSERT INTO wyprawy(szczyty_id, data_wyprawy, sukces, droga)
             VALUES(?,?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, wyprawa)
    conn.commit()
    return cur.lastrowid


if __name__ == "__main__":

    create_szczyty_sql = """
   -- szczyty table
   CREATE TABLE IF NOT EXISTS szczyty (
      id integer PRIMARY KEY,
      nazwa text NOT NULL,
      wysokosc_bezwzgledna integer,
      wybitnosc integer
   );
   """

    create_wyprawy_sql = """
   -- wyprawy table
   CREATE TABLE IF NOT EXISTS wyprawy (
      id integer PRIMARY KEY,
      szczyty_id integer NOT NULL,
      data_wyprawy text NOT NULL,
      sukces boolean NOT NULL,
      droga VARCHAR(250) NOT NULL,
      FOREIGN KEY (szczyty_id) REFERENCES szczyty (id)
   );
   """

    db_file = "database.db"

    conn = create_connection(db_file)
    if conn is not None:
        execute_sql(conn, create_szczyty_sql)
        execute_sql(conn, create_wyprawy_sql)
        conn.close()

    szczyt = ("Gerlach", 2655, 2355)
    conn = create_connection("database.db")
    szczyt_id = add_szczyt(conn, szczyt)

    wyprawa = (
        szczyt_id,
        "2022-07-14",
        True,
        "Próba Tatarki",
    )

    wyprawa_id = add_wyprawa(conn, wyprawa)

    print(szczyt_id, wyprawa_id)
    conn.commit()
