import pytest

from watchlist.database import MoviesTable


class TestMoviesTable:
    @pytest.fixture
    def movies_table(self):
        return MoviesTable('database\watchlist.db')

    def test_get_movie_by_ID(self, movies_table: MoviesTable):
        movie = movies_table.get_movie_by_ID('tt0133093')
        assert movie['primaryTitle'] == 'The Matrix'