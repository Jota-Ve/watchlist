import sqlite3 as sql3
from typing import Any


class MoviesTable:
    columns     = ('movieID', 'primaryTitle', 'originalTitle', 'yearRelease', 'runtimeMinutes', 'genres')
    row_factory = sql3.Row

    def __init__(self, connection: str) -> None:
        self.__connection_str = connection
        self._connection = sql3.Connection(self.__connection_str)
        self._connection.row_factory = self.row_factory
        self.__is_closed = False


    def _create_table(self):
        self._connection.execute(
            """CREATE TABLE IF NOT EXISTS "Movies" (
                "movieID"	TEXT,
                "primaryTitle"	TEXT NOT NULL,
                "originalTitle"	TEXT NOT NULL,
                "yearRelease"	INTEGER NOT NULL,
                "runtimeMinutes"	INTEGER NOT NULL,
                "genres"	TEXT NOT NULL,
                PRIMARY KEY("movieID")
            )"""
        )



    @property
    def is_closed(self): return self.__is_closed


    def connect(self, connection: str=''):
        if self.__is_closed:
            if connection and connection != self.__connection_str:
                self.__connection_str = connection

            self._connection = sql3.Connection(self.__connection_str)
            self._connection.row_factory = self.row_factory
            self.__is_closed = False


    def save_changes(self):
        self._connection.commit()


    def close(self):
        self._connection.close()
        self.__is_closed = True

    #region GET METHODS
    def get_movies(self) -> sql3.Cursor:
        return self._connection.execute("SELECT * FROM Movies")


    def get_movie_by_ID(self, ID: str) -> sql3.Row|None:
        res = self._connection.execute("""SELECT * FROM Movies WHERE movieID=:ID""",
                                      {'ID': ID})

        return res.fetchone()


    def get_movies_by_primary_title(self, title: str) -> sql3.Cursor:
        res = self._connection.execute("""SELECT * FROM Movies WHERE primaryTitle LIKE ?""",
                                      (title,))
        return res

    def get_movies_by_original_title(self, title: str) -> sql3.Cursor:
        res = self._connection.execute("""SELECT * FROM Movies WHERE originalTitle LIKE ?""",
                                      (title,))
        return res


    def get_movies_like_primary_title(self, title: str) -> sql3.Cursor:
        res = self._connection.execute("""SELECT * FROM Movies WHERE primaryTitle LIKE ?""",
                                      (f'%{title}%',))
        return res

    def get_movies_like_original_title(self, title: str) -> sql3.Cursor:
        res = self._connection.execute("""SELECT * FROM Movies WHERE originalTitle LIKE ?""",
                                      (f'%{title}%',))
        return res


    def get_movies_by_year_release(self, year: int) -> sql3.Cursor:
        res = self._connection.execute("""SELECT * FROM Movies WHERE yearRelease=?""",
                                      (year, ))
        return res


    def get_movies_by_runtime(self, minutes: int) -> sql3.Cursor:
        res = self._connection.execute("""SELECT * FROM Movies WHERE runtimeMinutes=?""",
                                      (minutes, ))
        return res


    def get_movies_by_genre(self, genre: str) -> sql3.Cursor:
        res = self._connection.execute("""SELECT * FROM Movies WHERE genres LIKE ?""",
                                      (f'%{genre}%', ))
        return res

    #endregion GET METHODS


    def _add_movie_dict(self, id_: str, movie: dict[str, Any]):
        SQL = """INSERT INTO Movies
                (movieID, primaryTitle, originalTitle, yearRelease, runtimeMinutes, genres)
                VALUES (:movieID, :primaryTitle, :originalTitle, :yearRelease, :runtimeMinutes, :genres)"""

        parameters = movie | {'movieID': id_}
        self._connection.execute(SQL, parameters)


    def _add_movie_tuple(self, id_, movie: tuple):
        SQL = """INSERT INTO Movies
                (movieID, primaryTitle, originalTitle, yearRelease, runtimeMinutes, genres)
                VALUES (?, ?, ?, ?, ?, ?)"""

        parameters = (id_,) + movie
        self._connection.execute(SQL, parameters)


    def add_movie(self, movie: tuple|dict) -> str:
        # Seleciona o "maior/último" id da tabela
        res = self._connection.execute("SELECT MAX(movieID) FROM Movies")
        last_id: str|None = res.fetchone()[0]

        if last_id is not None:
            new_id_number = int(last_id[2:]) + 1

        else: # Se o último id for None, a tabela está vazia
            res = self._connection.execute("SELECT COUNT(*) FROM Movies")
            assert res.fetchone()[0] == 0, ("A tabela possui registros, mas não "
                                            "foi possível localizar o último id.")
            new_id_number = 0

        new_id = f'tt{new_id_number:07}'
        if isinstance(movie, tuple):
            self._add_movie_tuple(new_id, movie)
        else:
            self._add_movie_dict(new_id, movie)

        return new_id