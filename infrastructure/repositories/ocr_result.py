from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from psycopg2.extras import Json
from infrastructure.database import get_db_cursor


@dataclass
class OcrResult:
    """OCR result entity representing processed document data."""

    id: int
    file_id: str
    result: Dict[str, Any]
    file_type: str
    tag: str
    metadata: Dict[str, Any]
    status: int


class OcrResultRepository:
    """Repository for OCR result database operations with SQL injection protection."""

    def insert(self, file_id: str, result: Dict[str, Any], file_type: str,
               tag: str, metadata: Dict[str, Any], status: int = 1) -> int:
        """
        Insert a new OCR result into the database.

        Args:
            file_id: Document file identifier
            result: OCR processing result as JSON
            file_type: Type of the file
            tag: Document tag/classification
            metadata: Additional metadata as JSON
            status: Processing status (default 1)

        Returns:
            ID of the inserted OCR result

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=True) as (conn, cursor):
            cursor.execute(
                """
                INSERT INTO ocr_results (file_id, result, file_type, tag, metadata, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (file_id, Json(result), file_type, tag, Json(metadata), status)
            )
            ocr_id = cursor.fetchone()[0]
            return ocr_id

    def insert_many(self, ocr_results: List[tuple[str, Dict, str, str, Dict, int]]) -> List[int]:
        """
        Bulk insert multiple OCR results into the database.

        Args:
            ocr_results: List of tuples (file_id, result, file_type, tag, metadata, status)

        Returns:
            List of inserted OCR result IDs

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=True) as (conn, cursor):
            data = [
                (file_id, Json(result), file_type, tag, Json(metadata), status)
                for file_id, result, file_type, tag, metadata, status in ocr_results
            ]

            cursor.executemany(
                """
                INSERT INTO ocr_results (file_id, result, file_type, tag, metadata, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                data
            )
            cursor.execute("SELECT lastval()")
            last_id = cursor.fetchone()[0]
            return list(range(last_id - len(ocr_results) + 1, last_id + 1))

    def get_by_id(self, ocr_id: int) -> Optional[OcrResult]:
        """
        Retrieve an OCR result by its ID.

        Args:
            ocr_id: OCR result identifier

        Returns:
            OcrResult object if found, None otherwise

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            cursor.execute(
                """
                SELECT id, file_id, result, file_type, tag, metadata, status
                FROM ocr_results
                WHERE id = %s
                """,
                (ocr_id,)
            )
            row = cursor.fetchone()
            if row:
                return OcrResult(*row)
            return None

    def get_by_file_id(self, file_id: str) -> Optional[OcrResult]:
        """
        Retrieve an OCR result by its file ID.

        Args:
            file_id: Document file identifier

        Returns:
            OcrResult object if found, None otherwise

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            cursor.execute(
                """
                SELECT id, file_id, result, file_type, tag, metadata, status
                FROM ocr_results
                WHERE file_id = %s
                """,
                (file_id,)
            )
            row = cursor.fetchone()
            if row:
                return OcrResult(*row)
            return None

    def get_by_tag(self, tag: str) -> List[OcrResult]:
        """
        Retrieve all OCR results with a specific tag.

        Args:
            tag: Document tag/classification

        Returns:
            List of OcrResult objects

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            cursor.execute(
                """
                SELECT id, file_id, result, file_type, tag, metadata, status
                FROM ocr_results
                WHERE tag = %s
                ORDER BY id
                """,
                (tag,)
            )
            rows = cursor.fetchall()
            return [OcrResult(*row) for row in rows]

    def get_by_status(self, status: int) -> List[OcrResult]:
        """
        Retrieve all OCR results with a specific status.

        Args:
            status: Processing status code

        Returns:
            List of OcrResult objects

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            cursor.execute(
                """
                SELECT id, file_id, result, file_type, tag, metadata, status
                FROM ocr_results
                WHERE status = %s
                ORDER BY id
                """,
                (status,)
            )
            rows = cursor.fetchall()
            return [OcrResult(*row) for row in rows]

    def get_all(self, limit: Optional[int] = None, offset: int = 0) -> List[OcrResult]:
        """
        Retrieve all OCR results with optional pagination.

        Args:
            limit: Maximum number of results to return (optional)
            offset: Number of results to skip (default 0)

        Returns:
            List of OcrResult objects

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            if limit is not None:
                cursor.execute(
                    """
                    SELECT id, file_id, result, file_type, tag, metadata, status
                    FROM ocr_results
                    ORDER BY id
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset)
                )
            else:
                cursor.execute(
                    """
                    SELECT id, file_id, result, file_type, tag, metadata, status
                    FROM ocr_results
                    ORDER BY id
                    """
                )
            rows = cursor.fetchall()
            return [OcrResult(*row) for row in rows]

    def update_status(self, file_id: str, status: int) -> bool:
        """
        Update the status of an OCR result.

        Args:
            file_id: Document file identifier
            status: New status code

        Returns:
            True if status was updated, False if not found

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=True) as (conn, cursor):
            cursor.execute(
                """
                UPDATE ocr_results
                SET status = %s
                WHERE file_id = %s
                """,
                (status, file_id)
            )
            return cursor.rowcount > 0

    def update_tag(self, file_id: str, tag: str) -> bool:
        """
        Update the tag of an OCR result.

        Args:
            file_id: Document file identifier
            tag: New tag/classification

        Returns:
            True if tag was updated, False if not found

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=True) as (conn, cursor):
            cursor.execute(
                """
                UPDATE ocr_results
                SET tag = %s
                WHERE file_id = %s
                """,
                (tag, file_id)
            )
            return cursor.rowcount > 0

    def delete(self, file_id: str) -> bool:
        """
        Delete an OCR result by its file ID.

        Args:
            file_id: Document file identifier

        Returns:
            True if result was deleted, False if not found

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=True) as (conn, cursor):
            cursor.execute(
                """
                DELETE FROM ocr_results
                WHERE file_id = %s
                """,
                (file_id,)
            )
            return cursor.rowcount > 0

    def exists(self, file_id: str) -> bool:
        """
        Check if an OCR result with the given file ID exists.

        Args:
            file_id: Document file identifier

        Returns:
            True if result exists, False otherwise

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            cursor.execute(
                """
                SELECT EXISTS(SELECT 1 FROM ocr_results WHERE file_id = %s)
                """,
                (file_id,)
            )
            return cursor.fetchone()[0]

    def count_by_tag(self, tag: str) -> int:
        """
        Count OCR results with a specific tag.

        Args:
            tag: Document tag/classification

        Returns:
            Number of results with the specified tag

        Raises:
            psycopg2.Error: If database operation fails
        """
        with get_db_cursor(commit=False) as (conn, cursor):
            cursor.execute(
                """
                SELECT COUNT(*) FROM ocr_results WHERE tag = %s
                """,
                (tag,)
            )
            return cursor.fetchone()[0]
