import sqlite3 as sql3


class MoviesTable:
    columns = ('movieID', 'primaryTitle', 'originalTitle', 'yearRelease', 'runtimeMinutes', 'genres')
    row_factory = sql3.Row

    def __init__(self, connection: str) -> None:
        self.__connection_str = connection
        self.connection = sql3.Connection(self.__connection_str)
        self.connection.row_factory = self.row_factory
        self.__is_closed = False


    @property
    def is_closed(self): return self.__is_closed


    def connect(self, connection: str=''):
        if self.__is_closed:
            if connection and connection != self.__connection_str:
                self.__connection_str = connection

            self.connection = sql3.Connection(self.__connection_str)
            self.connection.row_factory = self.row_factory
            self.__is_closed = False


    def close(self):
        self.connection.close()
        self.__is_closed = True


    def get_movie_by_ID(self, ID: str) -> sql3.Row|None:
        res = self.connection.execute("""SELECT * FROM Movies WHERE movieID=:ID""",
                                      {'ID': ID})

        return res.fetchone()


    def get_movies_by_primary_title(self, title: str) -> sql3.Cursor:
        res = self.connection.execute("""SELECT * FROM Movies WHERE primaryTitle LIKE ?""",
                                      (title,))
        return res

    def get_movies_by_original_title(self, title: str) -> sql3.Cursor:
        res = self.connection.execute("""SELECT * FROM Movies WHERE originalTitle LIKE ?""",
                                      (title,))
        return res


    def get_movies_like_primary_title(self, title: str) -> sql3.Cursor:
        res = self.connection.execute("""SELECT * FROM Movies WHERE primaryTitle LIKE ?""",
                                      (f'%{title}%',))
        return res

    def get_movies_like_original_title(self, title: str) -> sql3.Cursor:
        res = self.connection.execute("""SELECT * FROM Movies WHERE originalTitle LIKE ?""",
                                      (f'%{title}%',))
        return res


    def get_movies_by_year_release(self, year: int) -> sql3.Cursor:
        res = self.connection.execute("""SELECT * FROM Movies WHERE yearRelease=?""",
                                      (year, ))
        return res