import sqlite3 as sql3

import pytest

from watchlist.database import MoviesTable

DATABASE_PATH = r'database\watchlist.db'

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
        table = MoviesTable(':memory:')
        table._create_table()
        yield table
        table.close()


    @pytest.fixture()
    def action_movie(self):
        # ('movieID', 'primaryTitle', 'originalTitle', 'yearRelease', 'runtimeMinutes', 'genres')
        return ('Action Movie', 'Action Movie', 2000, 120, 'Action')



    def test_creating_table(self):
        table = MoviesTable(':memory:')
        table._create_table()
        SQL = """SELECT name FROM sqlite_master WHERE type='table' AND name='Movies'"""

        assert table._connection.execute(SQL).fetchone()



    def test_closing_connection(self, temp_movies_table: MoviesTable):
        temp_movies_table.close()

        with pytest.raises(sql3.ProgrammingError, match='Cannot operate on a closed database'):
            temp_movies_table.get_movie_by_ID('tt0133093')

    def test_reopening_connection(self, temp_movies_table: MoviesTable):
        temp_movies_table.close()
        temp_movies_table.connect(DATABASE_PATH)

        movie = temp_movies_table.get_movie_by_ID('tt0133093')
        assert movie and movie['primaryTitle'] == 'The Matrix'


    def test_property_is_closed_while_connection_open(self, temp_movies_table: MoviesTable):
        assert temp_movies_table.is_closed == False

    def test_property_is_closed_after_closing(self, temp_movies_table: MoviesTable):
        temp_movies_table.close()
        assert temp_movies_table.is_closed == True


    #region Test Get Methods
    def test_get_movie_by_ID(self, movies_table: MoviesTable):
        movie = movies_table.get_movie_by_ID('tt0133093')
        assert movie and movie['primaryTitle'] == 'The Matrix'

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


    def test_get_movies_by_genre(self, movies_table: MoviesTable):
        genre = 'ACTION'
        movies = movies_table.get_movies_by_genre(genre).fetchall()
        assert movies and all(genre in movie['genres'].upper() for movie in movies)

    def test_get_movies_by_genre_inexistent(self, movies_table: MoviesTable):
        genre = 'FAKE_GENRE'
        movies = movies_table.get_movies_by_genre(genre).fetchall()
        assert movies == []

    #endregion Test Get Methods


    def test_adding_new_movie_tuple(self, temp_movies_table: MoviesTable):
        # ('movieID', 'primaryTitle', 'originalTitle', 'yearRelease', 'runtimeMinutes', 'genres')
        new_movie = ('Fast and Furious 99', 'Fast and Furious 99', 2150, 120, 'Action')
        movie_id = temp_movies_table.add_movie(new_movie)
        returned_movie = temp_movies_table.get_movie_by_ID(movie_id)
        assert ((movie_id,) + new_movie) == tuple(returned_movie)


    def test_adding_new_movie_without_saving(self, temp_movies_table: MoviesTable, action_movie: tuple):
        temp_movies_table.add_movie(action_movie)
        assert temp_movies_table._connection.in_transaction

    def test_adding_new_movie_and_saving_changes(self, temp_movies_table: MoviesTable, action_movie: tuple):
        temp_movies_table.add_movie(action_movie)
        temp_movies_table.save_changes()
        assert not temp_movies_table._connection.in_transaction


    def test_adding_new_movie_dict(self, temp_movies_table: MoviesTable):
        # ('movieID', 'primaryTitle', 'originalTitle', 'yearRelease', 'runtimeMinutes', 'genres')
        new_movie = {'primaryTitle': 'Fast and Furious 99',
                     'originalTitle': 'Fast and Furious 99',
                     'yearRelease': 2150,
                     'runtimeMinutes': 120,
                     'genres': 'Action'}

        movie_id = temp_movies_table.add_movie(new_movie)
        returned_movie = dict(temp_movies_table.get_movie_by_ID(movie_id))
        returned_movie.pop('movieID')

        assert new_movie == returned_movie
