import importlib
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / 'src'))



def test_imports():
    MoviesTable = importlib.import_module('database', 'movie-watchlist.src').MoviesTable
    assert MoviesTable