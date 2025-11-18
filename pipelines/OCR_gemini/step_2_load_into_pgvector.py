import json
import sys
import csv
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.append(str(Path(__file__).parent.parent.parent))

from infrastructure.repositories import OcrResultRepository
from infrastructure.database import DatabaseConfig

TO_LOAD_DIR = Path(__file__).parent.parent.parent / 'gemini25flash'

CSV_FILE = Path(__file__).parent.parent.parent / 'download_status_with_tags.csv'


def load_csv_mappings() -> Dict[str, Tuple[str, str]]:
    """Load CSV file and create a mapping of filename -> (document_tag, file_type)."""
    mappings = {}

    if not CSV_FILE.exists():
        print(f"Warning: CSV file not found at {CSV_FILE}")
        return mappings

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            filename = row['filename']
            if filename.endswith('.pdf'):
                filename = filename[:-4]

            document_tag = row['document_tag']
            file_type = row['file_type']

            mappings[filename] = (document_tag, file_type)

    print(f"Loaded {len(mappings)} file mappings from CSV")
    return mappings


def load_document_pages(document_folder: Path) -> List[Dict]:
    """Load all JSON pages from a document folder and sort them by page number."""
    pages = []

    for json_file in document_folder.glob('*.json'):
        with open(json_file, 'r', encoding='utf-8') as f:
            page_data = json.load(f)
            pages.append(page_data)

    pages.sort(key=lambda x: x.get('metadata', {}).get('page_number', 0))

    return pages


def combine_document_pages(pages: List[Dict]) -> tuple:
    """Combine multiple pages into a single document OCR result and extract metadata."""
    combined_result = {
        'pages': []
    }

    ocr_engine = 'gemini-2.5-pro'
    file_type = None
    tag = None

    if pages and 'metadata' in pages[0]:
        page_metadata = pages[0]['metadata']
        ocr_engine = page_metadata.get('ocr_engine', 'gemini-2.5-flash')
        file_type = page_metadata.get('file_type')
        tag = page_metadata.get('tag')

    for page in pages:
        page_copy = page.copy()
        page_copy.pop('metadata', None)
        page_copy.pop('raw_response', None)

        combined_result['pages'].append(page_copy)

    document_metadata = {
        'ocr_engine': ocr_engine,
        'total_pages': len(pages)
    }

    return combined_result, file_type, tag, document_metadata


def insert_into_postgres(file_id: str, result: Dict, file_type: str, tag: str, metadata: Dict):
    """Insert a document into PostgreSQL using OcrResultRepository."""
    try:
        repo = OcrResultRepository()
        ocr_id = repo.insert(file_id, result, file_type, tag, metadata, status=1)
        print(f" Inserted document: {file_id} (ID: {ocr_id})")
    except Exception as e:
        print(f" Error inserting {file_id}: {e}")


def process_all_documents():
    """Process all documents in the to_load_into_db folder."""
    if not TO_LOAD_DIR.exists():
        print(f"Error: Directory {TO_LOAD_DIR} does not exist")
        return

    csv_mappings = load_csv_mappings()

    document_folders = [f for f in TO_LOAD_DIR.iterdir() if f.is_dir()]

    print(f"Found {len(document_folders)} documents to process")

    for doc_folder in document_folders:
        file_id = doc_folder.name

        pages = load_document_pages(doc_folder)

        if not pages:
            print(f"⊘ Skipping {file_id}: no JSON files found")
            continue

        result, file_type, tag, metadata = combine_document_pages(pages)

        if file_id in csv_mappings:
            csv_tag, csv_file_type = csv_mappings[file_id]
            tag = csv_tag
            file_type = csv_file_type
            print(f"  Using CSV data for {file_id}: tag={tag}, file_type={file_type}")
        else:
            print(f"  Warning: No CSV mapping found for {file_id}, using metadata values")

        insert_into_postgres(file_id, result, file_type, tag, metadata)

    print(f"\nProcessing complete! Processed {len(document_folders)} documents")


if __name__ == '__main__':
    config = DatabaseConfig()
    print("Starting OCR results upload to PostgreSQL...")
    print(f"Source directory: {TO_LOAD_DIR}")
    print(f"Database: {config.host}:{config.port}")
    print("-" * 60)

    process_all_documents()
