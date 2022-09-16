from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, Float, Date, MetaData
from sqlalchemy import update
from sqlalchemy import delete


engine = create_engine("sqlite:///database3.db", echo=True)

print(engine.driver)

meta = MetaData()

stations = Table(
    "stations",
    meta,
    Column("id", Integer, primary_key=True),
    Column("station_code", String),
    Column("latitude", String),
    Column("longitude", String),
    Column("elevation", Float),
    Column("name", String),
    Column("country", String),
    Column("state", String),
)

measure = Table(
    "measure",
    meta,
    Column("id", Integer, primary_key=True),
    Column("station_code", String),
    Column("date", String),
    Column("tobs", Integer),
)

meta.create_all(engine)
print(engine.table_names())


def conn_execute_with_print(conn, sql):
    result = conn.execute(sql)
    for row in result:
        print(row)


if __name__ == "__main__":

    ins = stations.insert()

    print(ins.compile().params)
    print()

    conn = engine.connect()
    conn.execute(
        ins,
        [
            {
                "station_code": "USC00519397",
                "latitude": "21.2716",
                "longitude": "-157.8168",
                "elevation": 3.0,
                "name": "Waikiki",
                "country": "US",
                "state": "HI",
            },
            {
                "station_code": "USC00513117",
                "latitude": "21.4234",
                "longitude": "-157.8015",
                "elevation": 20.0,
                "name": "Kaneohe",
                "country": "US",
                "state": "HI",
            },
            {
                "station_code": "USC00514830",
                "latitude": "21.5213",
                "longitude": "-157.8374",
                "elevation": 7.0,
                "name": "Kualoa",
                "country": "US",
                "state": "HI",
            },
        ],
    )

    ins2 = measure.insert()
    conn.execute(
        ins2,
        [
            {
                "station_code": "USC00519397",
                "date": "01/01/2010",
                "tobs": 65,
            },
            {
                "station_code": "USC00519397",
                "date": "02/01/2010",
                "tobs": 15,
            },
            {
                "station_code": "USC00519397",
                "date": "03/01/2010",
                "tobs": 72,
            },
        ],
    )

    s = stations.select().where(stations.c.elevation > 15)
    conn_execute_with_print(conn, s)
    print()

    result = conn.execute("SELECT * FROM measure LIMIT 2")
    rows = result.fetchall()
    print(rows)
    print()

    r = (
        stations.update()
        .where(stations.c.country == "US")
        .values(country="Stany Zjednoczone")
    )
    result = conn.execute(r)

    s = stations.select().where(stations.c.country == "US")
    conn_execute_with_print(conn, s)
    print()

    p = measure.delete().where(measure.c.date == "01/01/2010")
    result = conn.execute(p)

    k = measure.select().where(measure.c.date == "01/01/2010")
    conn_execute_with_print(conn, k)
    print()

    x = stations.delete()
    conn.execute(x)
    y = measure.delete()
    conn.execute(y)
