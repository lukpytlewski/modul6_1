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


def execute_sql(conn, sql, args=None):
    """Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        if args:
            c.execute(sql, args)
        else:
            c.execute(sql)
        return c
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
    cur = execute_sql(conn, sql, szczyt)
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
    cur = execute_sql(conn, sql, wyprawa)
    conn.commit()
    return cur.lastrowid


def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    sql = f"SELECT * FROM {table}"
    cur = execute_sql(conn, sql)
    rows = cur.fetchall()
    return rows


def select_where(conn, table, **query):
    """
    Query wyprawy from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur = execute_sql(conn, f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows


def update(conn, table, id, **kwargs):
    """
    update sukces and data_wyprawy of a wyprawy
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id,)

    sql = f""" UPDATE {table}
             SET {parameters}
             WHERE id = ?"""
    try:
        execute_sql(conn, sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)


def delete_where(conn, table, **kwargs):
    """
    Delete from table where attributes from
    :param conn:  Connection to the SQLite database
    :param table: table name
    :param kwargs: dict of attributes and values
    :return:
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f"DELETE FROM {table} WHERE {q}"
    execute_sql(conn, sql, values)
    conn.commit()
    print("Deleted")


def delete_all(conn, table):
    """
    Delete all rows from table
    :param conn: Connection to the SQLite database
    :param table: table name
    :return:
    """
    sql = f"DELETE FROM {table}"
    execute_sql(conn, sql)
    conn.commit()
    print("Deleted")


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

    szczyt = ("Gerlach", 2655, 2355)
    szczyt_id = add_szczyt(conn, szczyt)

    wyprawa = (
        szczyt_id,
        "2022-07-14",
        True,
        "Pr??ba Tatarki",
    )

    wyprawa_id = add_wyprawa(conn, wyprawa)
    add_wyprawa(conn, (szczyt_id, "2022-05-14", True, "lorem ipsum"))
    add_wyprawa(conn, (szczyt_id, "2017-11-13", False, "Ala ma kota"))

    # wszystkie szczyty
    print(select_all(conn, "szczyty"))
    print()

    # wszystkie wyprawy
    print(select_all(conn, "wyprawy"))
    print()

    # wszystkie wyprawy zako??czone sukcesem
    print(select_where(conn, "wyprawy", sukces=True))
    print()

    update(conn, "wyprawy", 2, sukces=False)
    # wszystkie wyprawy zako??czone sukcesem
    print(select_where(conn, "wyprawy", sukces=True))
    print()

    delete_where(conn, "wyprawy", id=3)
    # wszystkie wyprawy
    print(select_all(conn, "wyprawy"))
    print()

    delete_all(conn, "wyprawy")
    delete_all(conn, "szczyty")

    conn.close()
