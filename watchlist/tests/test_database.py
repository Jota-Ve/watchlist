import sqlite3 as sql3

import pytest

from watchlist.database import MoviesTable

DATABASE_PATH = 'database\watchlist.db'

class TestMoviesTable:
    @pytest.fixture(scope='class')
    def movies_table(self):
        """Instância única para toda a classe"""
        table = MoviesTable(DATABASE_PATH)
        yield table
        table.close()


    @pytest.fixture(scope='function')
    def temp_movies_table(self):
        """Nova instância para cada função"""
        table = MoviesTable(DATABASE_PATH)
        yield table
        table.close()


    def test_closing_connection(self, temp_movies_table: MoviesTable):
        temp_movies_table.close()

        with pytest.raises(sql3.ProgrammingError, match='Cannot operate on a closed database'):
            temp_movies_table.get_movie_by_ID('tt0133093')

    def test_reopening_connection(self, temp_movies_table: MoviesTable):
        temp_movies_table.close()
        temp_movies_table.connect(DATABASE_PATH)

        movie = temp_movies_table.get_movie_by_ID('tt0133093')
        assert movie['primaryTitle'] == 'The Matrix'


    def test_property_is_closed_while_connection_open(self, temp_movies_table: MoviesTable):
        assert temp_movies_table.is_closed == False

    def test_property_is_closed_after_closing(self, temp_movies_table: MoviesTable):
        temp_movies_table.close()
        assert temp_movies_table.is_closed == True


    def test_get_movie_by_ID(self, movies_table: MoviesTable):
        movie = movies_table.get_movie_by_ID('tt0133093')
        assert movie['primaryTitle'] == 'The Matrix'

    def test_get_movie_by_ID_inexistent(self, movies_table: MoviesTable):
        movie = movies_table.get_movie_by_ID('000000000')
        assert movie is None


    def test_get_movies_by_primary_title(self, movies_table: MoviesTable):
        title = 'ROCKY'
        movies = movies_table.get_movies_by_primary_title(title).fetchall()
        assert movies and all(title == movie['primaryTitle'].upper() for movie in movies)

    def test_get_movies_by_original_title(self, movies_table: MoviesTable):
        title = 'STAR WARS'
        movies = movies_table.get_movies_by_original_title(title).fetchall()
        assert movies and all(title == movie['originalTitle'].upper() for movie in movies)

    def test_get_movies_like_primary_title(self, movies_table: MoviesTable):
        title = 'ROCKY'
        movies = movies_table.get_movies_like_primary_title(title).fetchall()
        assert movies and all(title in movie['primaryTitle'].upper() for movie in movies)

    def test_get_movies_like_original_title(self, movies_table: MoviesTable):
        title = 'TERMINATOR'
        movies = movies_table.get_movies_like_original_title(title).fetchall()
        assert movies and all(title in movie['originalTitle'].upper() for movie in movies)


    def test_get_movies_by_year_release(self, movies_table: MoviesTable):
        movies = movies_table.get_movies_by_year_release(1999).fetchall()
        assert movies and all(movie['yearRelease'] == 1999 for movie in movies)


    def test_get_movies_by_runtime_minutes(self, movies_table: MoviesTable):
        movies = movies_table.get_movies_by_runtime(120).fetchall()
        assert movies and all(movie['runtimeMinutes'] == 120 for movie in movies)