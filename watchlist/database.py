import sqlite3 as sql3


class MoviesTable:
    columns = ('movieID', 'primaryTitle', 'originalTitle', 'yearRelease', 'runtimeMinutes', 'genres')

    def __init__(self, connection: str) -> None:
        self.connection = sql3.Connection(connection)
        self.connection.row_factory = sql3.Row

    def get_movie_by_ID(self, ID: str) -> sql3.Row:
        res = self.connection.execute("""SELECT * FROM Movies WHERE movieID=:id""",
                                      { 'id': ID})

        return res.fetchone()