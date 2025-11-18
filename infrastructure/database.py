import os
from contextlib import contextmanager
from typing import Generator
import psycopg2
from psycopg2.extensions import connection
from dotenv import load_dotenv

load_dotenv()


class DatabaseConfig:
    """Database configuration singleton."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = int(os.getenv('POSTGRES_PORT', '5431'))
        self.database = os.getenv('POSTGRES_DATABASE')
        self.user = os.getenv('POSTGRES_USER')
        self.password = os.getenv('POSTGRES_PASSWORD')

        self._initialized = True

    def to_dict(self) -> dict:
        """Convert configuration to dictionary format for psycopg2."""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }


@contextmanager
def get_db_connection() -> Generator[connection, None, None]:
    """
    Context manager for database connections with automatic cleanup.

    Yields:
        psycopg2 connection object

    Raises:
        psycopg2.Error: If connection fails
    """
    config = DatabaseConfig()
    conn = None

    try:
        conn = psycopg2.connect(**config.to_dict())
        yield conn
    finally:
        if conn is not None:
            conn.close()


@contextmanager
def get_db_cursor(commit: bool = True) -> Generator[tuple[connection, any], None, None]:
    """
    Context manager for database cursor with automatic transaction handling.

    Args:
        commit: Whether to commit the transaction on success

    Yields:
        Tuple of (connection, cursor)

    Raises:
        psycopg2.Error: If database operation fails
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            yield conn, cursor
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
