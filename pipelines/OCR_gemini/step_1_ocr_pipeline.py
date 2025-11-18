"""
Gemini 2.5 Flash OCR Pipeline via Vertex AI
Processes all PDFs with parallel OCR workers and CSV tracking.
"""
import sys
import json
import logging
import csv
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import fitz
import pandas as pd

pipeline_dir = Path(__file__).parent
sys.path.insert(0, str(pipeline_dir))
from aiolimiter import AsyncLimiter

from vertex_ai_client import VertexAIGeminiClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gemini_ocr_pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent
DOWNLOADED_PDFS_DIR = PROJECT_ROOT / "downloaded_pdfs"
OCR_RESULTS_DIR = PROJECT_ROOT / "gemini25flash"
TEMP_IMAGES_DIR = PROJECT_ROOT / "temp_page_images_gemini"
CSV_TRACKING_FILE = PROJECT_ROOT / "ocr_processing_status.csv"
OCR_ERRORS_FILE = PROJECT_ROOT / "gemini_ocr_errors.csv"

DPI = 300
IMAGE_FORMAT = "PNG"
MIN_TEXT_LENGTH = 50
MIN_WORDS = 10
MAX_WORKERS = 10  # Number of parallel workers per PDF (for pages)
MAX_PDF_WORKERS = 4  # Number of PDFs to process in parallel


class GeminiOCRPipeline:
    """Pipeline for processing PDFs through Gemini 2.5 flashOCR via Vertex AI."""

    def __init__(self, vertex_client: Optional[VertexAIGeminiClient] = None):
        self.vertex_client = vertex_client or VertexAIGeminiClient()
        self.errors: List[Dict[str, Any]] = []
        self.processing_status: List[Dict[str, Any]] = []

        OCR_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        TEMP_IMAGES_DIR.mkdir(parents=True, exist_ok=True)

        logger.info("Gemini 2.5 flashOCR Pipeline initialized")
        logger.info(f"PDFs directory: {DOWNLOADED_PDFS_DIR}")
        logger.info(f"Results directory: {OCR_RESULTS_DIR}")

        # Rate limiter: 1 request per 1 second
        self.rate_limiter = AsyncLimiter(max_rate=1, time_period=1)

    def check_page_has_text(self, page: fitz.Page) -> bool:
        """Check if a PDF page has extractable text."""
        text = page.get_text()
        text_cleaned = text.strip()
        word_count = len(text_cleaned.split())
        return len(text_cleaned) >= MIN_TEXT_LENGTH and word_count >= MIN_WORDS

    def extract_text_conventional(
        self,
        page: fitz.Page,
        pdf_name: str,
        page_num: int
    ) -> Dict[str, Any]:
        """Extract text from PDF page using conventional methods."""
        logger.info(f"Extracting text conventionally from {pdf_name} - Page {page_num}")

        try:
            text = page.get_text()
            text_dict = page.get_text("dict")

            headings = []
            font_sizes = {}

            for block in text_dict.get("blocks", []):
                if block.get("type") == 0:
                    for line in block.get("lines", []):
                        for span in line.get("spans", []):
                            size = span.get("size", 0)
                            text_content = span.get("text", "").strip()
                            if text_content:
                                if size not in font_sizes:
                                    font_sizes[size] = []
                                font_sizes[size].append(text_content)

            if font_sizes:
                sorted_sizes = sorted(font_sizes.keys(), reverse=True)
                for size in sorted_sizes[:2]:
                    if size > min(font_sizes.keys()) * 1.2:
                        headings.extend(font_sizes[size])

            tables = []
            images = []
            image_list = page.get_images()
            for idx, img in enumerate(image_list, 1):
                try:
                    bbox = page.get_image_bbox(img)
                    images.append({
                        "id": f"IMAGE_{idx}",
                        "bbox": [bbox.x0, bbox.y0, bbox.x1, bbox.y1],
                        "caption": "",
                        "description": "Image embedded in PDF (not analyzed)",
                        "type": "embedded",
                        "position": "unknown"
                    })
                except:
                    pass

            result = {
                'text': text,
                'headings': headings[:10],
                'tables': tables,
                'images': images,
                'structure': {
                    'has_headings': len(headings) > 0,
                    'has_tables': len(tables) > 0,
                    'has_lists': False,
                    'has_images': len(images) > 0,
                    'document_type': 'digital_pdf'
                },
                'metadata': {
                    'pdf_name': pdf_name,
                    'page_number': page_num,
                    'processed_at': datetime.now().isoformat(),
                    'extraction_method': 'conventional',
                    'dpi': None,
                    'ocr_engine': 'pymupdf'
                },
                'raw_response': None
            }

            logger.info(f"  ✓ Conventional extraction successful - {len(text)} chars extracted")
            return result

        except Exception as e:
            logger.error(f"  ✗ Conventional extraction failed: {e}")
            raise

    def convert_page_to_image(self, page: fitz.Page, output_path: Path) -> Path:
        """Convert a single PDF page to image."""
        zoom = DPI / 72
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        pix.save(str(output_path))
        return output_path

    async def _process_page_image_async(
        self,
        image_path: Path,
        pdf_name: str,
        page_num: int
    ) -> Optional[Dict[str, Any]]:
        """Process a single page image with rate limiting (async internal)."""
        async with self.rate_limiter:
            # Run the synchronous generate call in executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                self.vertex_client.generate,
                image_path,
                16384,
                0.0
            )

            result['metadata'] = {
                'pdf_name': pdf_name,
                'page_number': page_num,
                'processed_at': datetime.now().isoformat(),
                'image_path': str(image_path),
                'dpi': DPI,
                'extraction_method': 'gemini_2.5_flash_ocr',
                'ocr_engine': 'gemini-2.5-flash'
            }

            logger.info(f"   ✓ Gemini OCR successful - {len(result['text'])} chars extracted")
            return result

    def process_page_image(
        self,
        image_path: Path,
        pdf_name: str,
        page_num: int
    ) -> Optional[Dict[str, Any]]:
        """Process a single page image through Gemini 2.5 flashOCR via Vertex AI."""
        logger.info(f"Processing Gemini OCR for {pdf_name} - Page {page_num}")

        try:
            # Run async rate-limited processing in sync context
            result = asyncio.run(self._process_page_image_async(image_path, pdf_name, page_num))
            return result

        except Exception as e:
            logger.error(f"   ✗ Gemini OCR failed for page {page_num}: {e}")
            self.errors.append({
                'timestamp': datetime.now().isoformat(),
                'pdf_file': f"{pdf_name}.pdf",
                'page': page_num,
                'stage': 'gemini_ocr_processing',
                'error': str(e)
            })
            return None

    def save_page_result(
        self,
        result: Dict[str, Any],
        pdf_name: str,
        page_num: int
    ) -> None:
        """Save OCR result to JSON file."""
        pdf_results_dir = OCR_RESULTS_DIR / pdf_name
        pdf_results_dir.mkdir(parents=True, exist_ok=True)
        output_path = pdf_results_dir / f"{page_num}.json"

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"   Saved result to {output_path.relative_to(PROJECT_ROOT)}")
        except Exception as e:
            logger.error(f"   Failed to save result: {e}")
            self.errors.append({
                'timestamp': datetime.now().isoformat(),
                'pdf_file': f"{pdf_name}.pdf",
                'page': page_num,
                'stage': 'save_result',
                'error': str(e)
            })

    def process_single_page(
        self,
        pdf_path: Path,
        pdf_name: str,
        page_num: int,
        pdf_temp_dir: Path
    ) -> Dict[str, Any]:
        """Process a single page with OCR."""
        page_index = page_num + 1

        # Check if this page already has a result
        pdf_results_dir = OCR_RESULTS_DIR / pdf_name
        result_file = pdf_results_dir / f"{page_index}.json"

        if result_file.exists():
            logger.info(f"Page {page_index}: Skipping (already processed)")
            return {'page': page_index, 'success': True, 'error': None, 'skipped': True}

        try:
            pdf_document = fitz.open(pdf_path)
            page = pdf_document[page_num]

            logger.info(f"Page {page_index}: Using Gemini OCR")
            image_path = pdf_temp_dir / f"page_{page_index}.png"
            self.convert_page_to_image(page, image_path)
            result = self.process_page_image(image_path, pdf_name, page_index)

            pdf_document.close()

            if result:
                self.save_page_result(result, pdf_name, page_index)
                return {'page': page_index, 'success': True, 'error': None, 'skipped': False}
            else:
                return {'page': page_index, 'success': False, 'error': 'OCR returned no result', 'skipped': False}

        except Exception as e:
            logger.error(f"Page {page_index}: Failed to process - {e}")
            return {'page': page_index, 'success': False, 'error': str(e), 'skipped': False}

    def is_pdf_already_processed(self, pdf_path: Path) -> tuple[bool, int, int]:
        """
        Check if a PDF has been fully processed.
        Returns: (is_complete, total_pages, existing_pages_count)
        """
        pdf_name = pdf_path.stem
        pdf_results_dir = OCR_RESULTS_DIR / pdf_name

        if not pdf_results_dir.exists():
            return False, 0, 0

        # Get total pages from PDF
        try:
            pdf_document = fitz.open(pdf_path)
            total_pages = len(pdf_document)
            pdf_document.close()
        except Exception as e:
            logger.error(f"Failed to open PDF {pdf_path.name} to check page count: {e}")
            return False, 0, 0

        # Count existing JSON result files (pages are numbered from 1)
        existing_pages = list(pdf_results_dir.glob("*.json"))
        existing_page_count = len(existing_pages)

        # Check if all pages are processed
        is_complete = existing_page_count >= total_pages

        return is_complete, total_pages, existing_page_count

    def process_pdf(self, pdf_path: Path) -> Dict[str, Any]:
        """Process a single PDF file using parallel OCR workers."""
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing PDF: {pdf_path.name}")
        logger.info(f"{'='*60}")

        pdf_name = pdf_path.stem

        # Check if PDF is already fully processed
        is_complete, total_pages, existing_pages = self.is_pdf_already_processed(pdf_path)

        if is_complete:
            logger.info(f"✓ SKIPPING {pdf_path.name}: Already fully processed ({existing_pages}/{total_pages} pages)")
            logger.info(f"{'='*60}\n")
            return {
                'pdf_name': pdf_name,
                'pdf_file': pdf_path.name,
                'total_pages': total_pages,
                'successful_pages': existing_pages,
                'failed_pages': 0,
                'conventional_pages': 0,
                'gemini_ocr_pages': existing_pages,
                'start_time': datetime.now().isoformat(),
                'end_time': datetime.now().isoformat(),
                'status': 'skipped_already_processed'
            }

        if existing_pages > 0:
            logger.info(f"⚠ RESUMING {pdf_path.name}: Found {existing_pages}/{total_pages} existing pages, processing remaining")

        stats = {
            'pdf_name': pdf_name,
            'pdf_file': pdf_path.name,
            'total_pages': 0,
            'successful_pages': 0,
            'failed_pages': 0,
            'conventional_pages': 0,
            'gemini_ocr_pages': 0,
            'skipped_pages': 0,
            'start_time': datetime.now().isoformat(),
            'status': 'processing'
        }

        try:
            pdf_document = fitz.open(pdf_path)
            stats['total_pages'] = len(pdf_document)
            pdf_document.close()

            logger.info(f"Processing {stats['total_pages']} pages from {pdf_path.name} with {MAX_WORKERS} workers")

            pdf_temp_dir = TEMP_IMAGES_DIR / pdf_name
            pdf_temp_dir.mkdir(parents=True, exist_ok=True)

            # Process pages in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(
                        self.process_single_page,
                        pdf_path,
                        pdf_name,
                        page_num,
                        pdf_temp_dir
                    ): page_num
                    for page_num in range(stats['total_pages'])
                }

                for future in as_completed(futures):
                    page_result = future.result()

                    if page_result['success']:
                        stats['successful_pages'] += 1
                        if page_result.get('skipped', False):
                            stats['skipped_pages'] += 1
                        else:
                            stats['gemini_ocr_pages'] += 1
                    else:
                        stats['failed_pages'] += 1
                        self.errors.append({
                            'timestamp': datetime.now().isoformat(),
                            'pdf_file': pdf_path.name,
                            'page': page_result['page'],
                            'stage': 'page_processing',
                            'error': page_result['error']
                        })

            stats['end_time'] = datetime.now().isoformat()
            stats['status'] = 'completed' if stats['failed_pages'] == 0 else 'completed_with_errors'

        except Exception as e:
            logger.error(f"Failed to process PDF {pdf_path.name}: {e}")
            stats['status'] = 'failed'
            stats['end_time'] = datetime.now().isoformat()
            self.errors.append({
                'timestamp': datetime.now().isoformat(),
                'pdf_file': pdf_path.name,
                'page': 'N/A',
                'stage': 'pdf_open',
                'error': str(e)
            })

        logger.info(f"\n{'='*60}")
        logger.info(f"PDF Processing Complete: {pdf_name}")
        logger.info(f"  Total pages: {stats['total_pages']}")
        logger.info(f"  Successful: {stats['successful_pages']} (Gemini OCR: {stats['gemini_ocr_pages']}, Skipped: {stats['skipped_pages']})")
        logger.info(f"  Failed: {stats['failed_pages']}")
        logger.info(f"{'='*60}\n")

        self.processing_status.append(stats)
        return stats

    def process_all_pdfs(self, cleanup_temp: bool = True) -> Dict[str, Any]:
        """Process all PDFs in the downloaded_pdfs directory."""
        logger.info(f"\n{'#'*60}")
        logger.info(f"Starting Gemini 2.5 Flash OCR Pipeline")
        logger.info(f"  PDF parallelism: {MAX_PDF_WORKERS} PDFs at once")
        logger.info(f"  Page parallelism: {MAX_WORKERS} pages per PDF")
        logger.info(f"{'#'*60}\n")

        pdf_files = list(DOWNLOADED_PDFS_DIR.glob("*.pdf"))

        if not pdf_files:
            logger.warning(f"No PDF files found in {DOWNLOADED_PDFS_DIR}")
            return {'total_pdfs': 0}

        logger.info(f"Found {len(pdf_files)} PDF files to process")

        overall_stats = {
            'total_pdfs': len(pdf_files),
            'processed_pdfs': 0,
            'skipped_pdfs': 0,
            'total_pages': 0,
            'successful_pages': 0,
            'failed_pages': 0,
            'conventional_pages': 0,
            'gemini_ocr_pages': 0,
            'skipped_pages': 0,
            'start_time': datetime.now().isoformat(),
            'pdf_details': []
        }

        # Process PDFs in parallel
        with ThreadPoolExecutor(max_workers=MAX_PDF_WORKERS) as executor:
            futures = {
                executor.submit(self.process_pdf, pdf_path): pdf_path
                for pdf_path in pdf_files
            }

            for future in as_completed(futures):
                pdf_path = futures[future]
                try:
                    stats = future.result()
                    overall_stats['pdf_details'].append(stats)
                    overall_stats['processed_pdfs'] += 1
                    if stats.get('status') == 'skipped_already_processed':
                        overall_stats['skipped_pdfs'] += 1
                    overall_stats['total_pages'] += stats['total_pages']
                    overall_stats['successful_pages'] += stats['successful_pages']
                    overall_stats['failed_pages'] += stats['failed_pages']
                    overall_stats['conventional_pages'] += stats.get('conventional_pages', 0)
                    overall_stats['gemini_ocr_pages'] += stats.get('gemini_ocr_pages', 0)
                    overall_stats['skipped_pages'] += stats.get('skipped_pages', 0)
                except Exception as e:
                    logger.error(f"Failed to process PDF {pdf_path.name}: {e}")
                    self.errors.append({
                        'timestamp': datetime.now().isoformat(),
                        'pdf_file': pdf_path.name,
                        'page': 'N/A',
                        'stage': 'pdf_processing',
                        'error': str(e)
                    })

        overall_stats['end_time'] = datetime.now().isoformat()

        if self.errors:
            self.save_error_log()

        self.save_csv_tracking()

        if cleanup_temp:
            self.cleanup_temp_images()

        self.save_summary(overall_stats)
        self.print_summary(overall_stats)

        return overall_stats

    def save_error_log(self) -> None:
        """Save errors to CSV file."""
        if not self.errors:
            return

        try:
            df = pd.DataFrame(self.errors)
            df.to_csv(OCR_ERRORS_FILE, index=False)
            logger.info(f"Saved {len(self.errors)} errors to {OCR_ERRORS_FILE}")
        except Exception as e:
            logger.error(f"Failed to save error log: {e}")

    def save_csv_tracking(self) -> None:
        """Save processing status to CSV file."""
        try:
            with open(CSV_TRACKING_FILE, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['pdf_file', 'pdf_name', 'total_pages', 'successful_pages',
                             'failed_pages', 'conventional_pages', 'gemini_ocr_pages',
                             'skipped_pages', 'status', 'start_time', 'end_time']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for status in self.processing_status:
                    writer.writerow({k: status.get(k, '') for k in fieldnames})
            logger.info(f"Saved processing status to {CSV_TRACKING_FILE}")
        except Exception as e:
            logger.error(f"Failed to save CSV tracking: {e}")

    def cleanup_temp_images(self) -> None:
        """Delete temporary page images."""
        try:
            import shutil
            if TEMP_IMAGES_DIR.exists():
                shutil.rmtree(TEMP_IMAGES_DIR)
                logger.info("Cleaned up temporary page images")
        except Exception as e:
            logger.warning(f"Failed to cleanup temp images: {e}")

    def save_summary(self, stats: Dict[str, Any]) -> None:
        """Save processing summary to JSON."""
        summary_path = OCR_RESULTS_DIR / "_processing_summary.json"
        try:
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved processing summary to {summary_path}")
        except Exception as e:
            logger.error(f"Failed to save summary: {e}")

    def print_summary(self, stats: Dict[str, Any]) -> None:
        """Print final summary."""
        print("\n" + "=" * 70)
        print("GEMINI 2.5 FLASH OCR PIPELINE COMPLETE")
        print("=" * 70)
        print(f"Parallelism: {MAX_PDF_WORKERS} PDFs x {MAX_WORKERS} pages/PDF")
        print(f"Total PDFs: {stats['total_pdfs']} (Skipped: {stats.get('skipped_pdfs', 0)})")
        print(f"Total pages: {stats['total_pages']}")
        print(f"   Successful: {stats['successful_pages']} (Gemini OCR: {stats['gemini_ocr_pages']}, Skipped: {stats.get('skipped_pages', 0)})")
        print(f"   Failed: {stats['failed_pages']}")
        if stats['total_pages'] > 0:
            print(f"\nSuccess rate: {stats['successful_pages']/stats['total_pages']*100:.1f}%")
        print(f"\nResults saved to: {OCR_RESULTS_DIR}")
        print(f"CSV tracking: {CSV_TRACKING_FILE}")
        if self.errors:
            print(f"Errors logged to: {OCR_ERRORS_FILE}")
        print("=" * 70 + "\n")


def main():
    """Main entry point for the Gemini OCR pipeline."""
    try:
        logger.info("Initializing Gemini 2.5 flashOCR Pipeline...")
        pipeline = GeminiOCRPipeline()

        if not pipeline.vertex_client.test_connection():
            logger.error("Failed to connect to Vertex AI. Check your credentials and configuration.")
            sys.exit(1)

        stats = pipeline.process_all_pdfs(cleanup_temp=True)

        if stats.get('failed_pages', 0) > 0:
            logger.warning("Pipeline completed with some errors")
            sys.exit(1)
        else:
            logger.info("Pipeline completed successfully")
            sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
