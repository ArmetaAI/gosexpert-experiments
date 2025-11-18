from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from infrastructure.database import get_db_cursor


@dataclass
class Tag:
    """Tag entity representing a document classification tag."""

    id: int
    code: str
    name_ru: str
    name_kz: str
    create_date: date
    status: int


class TagRepository:
    """Repository for tag database operations with SQL injection protection."""

    def insert(self, code: str, name_ru: str, name_kz: str, status: int = 0) -> int:
        """
        Insert a new tag into the database.

        Args:
            code: Tag code identifier
            name_ru: Russian name
            name_kz: Kazakh name
            status: Tag status (default 0)

        Returns:
            ID of the inserted tag

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=True) as (conn, cursor):
            cursor.execute(
                """
                INSERT INTO public.tags (code, name_ru, name_kz, create_date, status)
                VALUES (%s, %s, %s, CURRENT_DATE, %s)
                RETURNING id
                """,
                (code, name_ru, name_kz, status)
            )
            tag_id = cursor.fetchone()[0]
            return tag_id

    def insert_many(self, tags: List[tuple[str, str, str, int]]) -> List[int]:
        """
        Bulk insert multiple tags into the database.

        Args:
            tags: List of tuples (code, name_ru, name_kz, status)

        Returns:
            List of inserted tag IDs

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=True) as (conn, cursor):
            cursor.executemany(
                """
                INSERT INTO public.tags (code, name_ru, name_kz, create_date, status)
                VALUES (%s, %s, %s, CURRENT_DATE, %s)
                """,
                tags
            )
            cursor.execute("SELECT lastval()")
            last_id = cursor.fetchone()[0]
            return list(range(last_id - len(tags) + 1, last_id + 1))

    def get_by_id(self, tag_id: int) -> Optional[Tag]:
        """
        Retrieve a tag by its ID.

        Args:
            tag_id: Tag identifier

        Returns:
            Tag object if found, None otherwise

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            cursor.execute(
                """
                SELECT id, code, name_ru, name_kz, create_date, status
                FROM public.tags
                WHERE id = %s
                """,
                (tag_id,)
            )
            row = cursor.fetchone()
            if row:
                return Tag(*row)
            return None

    def get_by_code(self, code: str) -> Optional[Tag]:
        """
        Retrieve a tag by its code.

        Args:
            code: Tag code identifier

        Returns:
            Tag object if found, None otherwise

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            cursor.execute(
                """
                SELECT id, code, name_ru, name_kz, create_date, status
                FROM public.tags
                WHERE code = %s
                """,
                (code,)
            )
            row = cursor.fetchone()
            if row:
                return Tag(*row)
            return None

    def get_all(self) -> List[Tag]:
        """
        Retrieve all tags from the database.

        Returns:
            List of Tag objects

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            cursor.execute(
                """
                SELECT id, code, name_ru, name_kz, create_date, status
                FROM public.tags
                ORDER BY id
                """
            )
            rows = cursor.fetchall()
            return [Tag(*row) for row in rows]

    def update(self, tag_id: int, code: Optional[str] = None,
               name_ru: Optional[str] = None, name_kz: Optional[str] = None,
               status: Optional[int] = None) -> bool:
        """
        Update an existing tag.

        Args:
            tag_id: Tag identifier
            code: New code (optional)
            name_ru: New Russian name (optional)
            name_kz: New Kazakh name (optional)
            status: New status (optional)

        Returns:
            True if tag was updated, False if not found

        Raises:
            psycopg2.Error: If database operation fails
        """
        updates = []
        params = []

        if code is not None:
            updates.append("code = %s")
            params.append(code)
        if name_ru is not None:
            updates.append("name_ru = %s")
            params.append(name_ru)
        if name_kz is not None:
            updates.append("name_kz = %s")
            params.append(name_kz)
        if status is not None:
            updates.append("status = %s")
            params.append(status)

        if not updates:
            return False

        params.append(tag_id)

        with get_db_cursor(commit=True) as (conn, cursor):
            cursor.execute(
                f"""
                UPDATE public.tags
                SET {', '.join(updates)}
                WHERE id = %s
                """,
                params
            )
            return cursor.rowcount > 0

    def delete(self, tag_id: int) -> bool:
        """
        Delete a tag by its ID.

        Args:
            tag_id: Tag identifier

        Returns:
            True if tag was deleted, False if not found

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=True) as (conn, cursor):
            cursor.execute(
                """
                DELETE FROM public.tags
                WHERE id = %s
                """,
                (tag_id,)
            )
            return cursor.rowcount > 0

    def exists(self, code: str) -> bool:
        """
        Check if a tag with the given code exists.

        Args:
            code: Tag code identifier

        Returns:
            True if tag exists, False otherwise

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            cursor.execute(
                """
                SELECT EXISTS(SELECT 1 FROM public.tags WHERE code = %s)
                """,
                (code,)
            )
            return cursor.fetchone()[0]
