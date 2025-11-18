"""
Step 2: DeepSeek OCR Processing Pipeline via Vertex AI
Processes files that can't be read conventionally using DeepSeek OCR through Vertex AI.

This script:
1. Scans all PDFs in the downloaded_pdfs directory
2. For each page, checks if text can be extracted conventionally
3. If text is extractable: uses PyMuPDF text extraction (fast, preserves structure)
4. If text is NOT extractable (scanned/image-based): uses DeepSeek OCR via Vertex AI
5. Saves results as individual JSON files per page in deepseek-ocr-results/
6. Logs any errors to ocr_errors.csv
"""
import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import fitz  # PyMuPDF
import pandas as pd

# Add the pipeline directory to path to import vertex_ai_client module
pipeline_dir = Path(__file__).parent
sys.path.insert(0, str(pipeline_dir))

from vertex_ai_client import VertexAIDeepSeekClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deepseek_ocr_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent.parent
DOWNLOADED_PDFS_DIR = PROJECT_ROOT / "downloaded_pdfs"
DEEPSEEK_OCR_RESULTS_DIR = PROJECT_ROOT / "deepseek-ocr-results"
OCR_ERRORS_FILE = PROJECT_ROOT / "deepseek_ocr_errors.csv"
TEMP_IMAGES_DIR = PROJECT_ROOT / "temp_page_images_deepseek"

# OCR settings
DPI = 300  # High quality for better OCR results
IMAGE_FORMAT = "PNG"  # PNG for best quality, lossless

# Text extraction threshold
MIN_TEXT_LENGTH = 50  # Minimum chars to consider page as having extractable text
MIN_WORDS = 10  # Minimum words to consider page as having extractable text


class DeepSeekOCRPipeline:
    """Pipeline for processing PDFs through DeepSeek OCR via Vertex AI."""

    def __init__(self, vertex_client: Optional[VertexAIDeepSeekClient] = None):
        """
        Initialize the DeepSeek OCR pipeline.

        Args:
            vertex_client: Optional pre-configured VertexAIDeepSeekClient instance
        """
        self.vertex_client = vertex_client or VertexAIDeepSeekClient()
        self.errors: List[Dict[str, Any]] = []

        # Create necessary directories
        DEEPSEEK_OCR_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        TEMP_IMAGES_DIR.mkdir(parents=True, exist_ok=True)

        logger.info("DeepSeek OCR Pipeline (Vertex AI) initialized")
        logger.info(f"PDFs directory: {DOWNLOADED_PDFS_DIR}")
        logger.info(f"Results directory: {DEEPSEEK_OCR_RESULTS_DIR}")

    def check_page_has_text(self, page: fitz.Page) -> bool:
        """
        Check if a PDF page has extractable text.

        Args:
            page: PyMuPDF page object

        Returns:
            True if page has sufficient extractable text, False otherwise
        """
        text = page.get_text()

        # Remove whitespace and count
        text_cleaned = text.strip()
        word_count = len(text_cleaned.split())

        # Check both character count and word count
        has_text = (
            len(text_cleaned) >= MIN_TEXT_LENGTH and
            word_count >= MIN_WORDS
        )

        return has_text

    def extract_text_conventional(
        self,
        page: fitz.Page,
        pdf_name: str,
        page_num: int
    ) -> Dict[str, Any]:
        """
        Extract text from PDF page using conventional methods.

        Args:
            page: PyMuPDF page object
            pdf_name: Original PDF filename (without extension)
            page_num: Page number (1-indexed)

        Returns:
            Structured result dictionary similar to OCR output
        """
        logger.info(f"Extracting text conventionally from {pdf_name} - Page {page_num}")

        try:
            # Extract text
            text = page.get_text()

            # Extract text with formatting (dict format gives more structure)
            text_dict = page.get_text("dict")

            # Try to identify headings (larger font sizes)
            headings = []
            font_sizes = {}

            for block in text_dict.get("blocks", []):
                if block.get("type") == 0:  # Text block
                    for line in block.get("lines", []):
                        for span in line.get("spans", []):
                            size = span.get("size", 0)
                            text_content = span.get("text", "").strip()
                            if text_content:
                                if size not in font_sizes:
                                    font_sizes[size] = []
                                font_sizes[size].append(text_content)

            # Identify headings as text with larger font sizes
            if font_sizes:
                sorted_sizes = sorted(font_sizes.keys(), reverse=True)
                # Take top 2 font sizes as potential headings
                for size in sorted_sizes[:2]:
                    if size > min(font_sizes.keys()) * 1.2:  # At least 20% larger
                        headings.extend(font_sizes[size])

            # Extract tables (basic detection)
            tables = []

            # Extract images/figures info
            images = []
            image_list = page.get_images()
            for idx, img in enumerate(image_list, 1):
                try:
                    xref = img[0]
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

            # Build result structure
            result = {
                'text': text,
                'headings': headings[:10],  # Limit to top 10
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
        """
        Convert a single PDF page to image.

        Args:
            page: PyMuPDF page object
            output_path: Path to save the image

        Returns:
            Path to the saved image
        """
        # Render page to image with high DPI
        zoom = DPI / 72  # 72 is default DPI
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)

        # Save as PNG
        pix.save(str(output_path))

        return output_path

    def process_page_image(
        self,
        image_path: Path,
        pdf_name: str,
        page_num: int
    ) -> Optional[Dict[str, Any]]:
        """
        Process a single page image through DeepSeek OCR via Vertex AI.

        Args:
            image_path: Path to the page image
            pdf_name: Original PDF filename (without extension)
            page_num: Page number (1-indexed)

        Returns:
            OCR result dictionary or None if failed
        """
        logger.info(f"Processing DeepSeek OCR for {pdf_name} - Page {page_num}")

        try:
            # Run OCR via Vertex AI
            result = self.vertex_client.generate(
                image_path,
                max_tokens=8192,  # Large token limit for complex documents
                temperature=0.0   # Most deterministic output
            )

            # Add metadata
            result['metadata'] = {
                'pdf_name': pdf_name,
                'page_number': page_num,
                'processed_at': datetime.now().isoformat(),
                'image_path': str(image_path),
                'dpi': DPI,
                'extraction_method': 'deepseek_ocr_vertex_ai',
                'ocr_engine': 'deepseek'
            }

            logger.info(f"   ✓ DeepSeek OCR successful - {len(result['text'])} chars extracted")
            return result

        except Exception as e:
            logger.error(f"   ✗ DeepSeek OCR failed for page {page_num}: {e}")
            self.errors.append({
                'timestamp': datetime.now().isoformat(),
                'pdf_file': f"{pdf_name}.pdf",
                'page': page_num,
                'stage': 'deepseek_ocr_processing',
                'error': str(e)
            })
            return None

    def save_page_result(
        self,
        result: Dict[str, Any],
        pdf_name: str,
        page_num: int
    ) -> None:
        """
        Save OCR result to JSON file.

        Args:
            result: OCR result dictionary
            pdf_name: Original PDF filename (without extension)
            page_num: Page number (1-indexed)
        """
        # Create directory for this PDF
        pdf_results_dir = DEEPSEEK_OCR_RESULTS_DIR / pdf_name
        pdf_results_dir.mkdir(parents=True, exist_ok=True)

        # Save as page_{num}.json
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

    def process_pdf(self, pdf_path: Path) -> Dict[str, Any]:
        """
        Process a single PDF file using hybrid extraction approach.
        Uses conventional extraction when possible, DeepSeek OCR when necessary.

        Args:
            pdf_path: Path to the PDF file

        Returns:
            Summary statistics for this PDF
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing PDF: {pdf_path.name}")
        logger.info(f"{'='*60}")

        pdf_name = pdf_path.stem
        stats = {
            'pdf_name': pdf_name,
            'total_pages': 0,
            'successful_pages': 0,
            'failed_pages': 0,
            'conventional_pages': 0,
            'deepseek_ocr_pages': 0,
            'start_time': datetime.now().isoformat()
        }

        try:
            # Open PDF
            pdf_document = fitz.open(pdf_path)
            stats['total_pages'] = len(pdf_document)

            logger.info(f"Processing {stats['total_pages']} pages from {pdf_path.name}")

            # Create temp directory for this PDF (for OCR pages)
            pdf_temp_dir = TEMP_IMAGES_DIR / pdf_name
            pdf_temp_dir.mkdir(parents=True, exist_ok=True)

            # Process each page
            for page_num in range(stats['total_pages']):
                page_index = page_num + 1  # 1-indexed for display

                try:
                    page = pdf_document[page_num]

                    # Check if page has extractable text
                    has_text = self.check_page_has_text(page)

                    result = None

                    if has_text:
                        # Use conventional extraction
                        logger.info(f"Page {page_index}: Text detected, using conventional extraction")
                        result = self.extract_text_conventional(page, pdf_name, page_index)
                        stats['conventional_pages'] += 1
                    else:
                        # Use DeepSeek OCR
                        logger.info(f"Page {page_index}: No text detected, using DeepSeek OCR")

                        # Convert page to image
                        image_path = pdf_temp_dir / f"page_{page_index}.png"
                        self.convert_page_to_image(page, image_path)

                        # Run DeepSeek OCR
                        result = self.process_page_image(image_path, pdf_name, page_index)
                        if result:
                            stats['deepseek_ocr_pages'] += 1

                    # Save result if successful
                    if result:
                        self.save_page_result(result, pdf_name, page_index)
                        stats['successful_pages'] += 1
                    else:
                        stats['failed_pages'] += 1

                except Exception as e:
                    logger.error(f"Page {page_index}: Failed to process - {e}")
                    stats['failed_pages'] += 1
                    self.errors.append({
                        'timestamp': datetime.now().isoformat(),
                        'pdf_file': pdf_path.name,
                        'page': page_index,
                        'stage': 'page_processing',
                        'error': str(e)
                    })

            pdf_document.close()
            stats['end_time'] = datetime.now().isoformat()

        except Exception as e:
            logger.error(f"Failed to process PDF {pdf_path.name}: {e}")
            self.errors.append({
                'timestamp': datetime.now().isoformat(),
                'pdf_file': pdf_path.name,
                'page': 'N/A',
                'stage': 'pdf_open',
                'error': str(e)
            })

        # Summary
        logger.info(f"\n{'='*60}")
        logger.info(f"PDF Processing Complete: {pdf_name}")
        logger.info(f"  Total pages: {stats['total_pages']}")
        logger.info(f"  Successful: {stats['successful_pages']} (Conventional: {stats['conventional_pages']}, DeepSeek OCR: {stats['deepseek_ocr_pages']})")
        logger.info(f"  Failed: {stats['failed_pages']}")
        logger.info(f"{'='*60}\n")

        return stats

    def process_all_pdfs(self, cleanup_temp: bool = True) -> Dict[str, Any]:
        """
        Process all PDFs in the downloaded_pdfs directory.

        Args:
            cleanup_temp: Whether to delete temporary page images after processing

        Returns:
            Overall statistics and summary
        """
        logger.info(f"\n{'#'*60}")
        logger.info("Starting DeepSeek OCR Pipeline (Vertex AI) for All PDFs")
        logger.info(f"{'#'*60}\n")

        # Get all PDF files
        pdf_files = list(DOWNLOADED_PDFS_DIR.glob("*.pdf"))

        if not pdf_files:
            logger.warning(f"No PDF files found in {DOWNLOADED_PDFS_DIR}")
            return {'total_pdfs': 0}

        logger.info(f"Found {len(pdf_files)} PDF files to process")

        # Overall statistics
        overall_stats = {
            'total_pdfs': len(pdf_files),
            'processed_pdfs': 0,
            'total_pages': 0,
            'successful_pages': 0,
            'failed_pages': 0,
            'conventional_pages': 0,
            'deepseek_ocr_pages': 0,
            'start_time': datetime.now().isoformat(),
            'pdf_details': []
        }

        # Process each PDF
        for pdf_path in pdf_files:
            stats = self.process_pdf(pdf_path)
            overall_stats['pdf_details'].append(stats)
            overall_stats['processed_pdfs'] += 1
            overall_stats['total_pages'] += stats['total_pages']
            overall_stats['successful_pages'] += stats['successful_pages']
            overall_stats['failed_pages'] += stats['failed_pages']
            overall_stats['conventional_pages'] += stats.get('conventional_pages', 0)
            overall_stats['deepseek_ocr_pages'] += stats.get('deepseek_ocr_pages', 0)

        overall_stats['end_time'] = datetime.now().isoformat()

        # Save error log
        if self.errors:
            self.save_error_log()

        # Cleanup temporary images
        if cleanup_temp:
            self.cleanup_temp_images()

        # Save overall summary
        self.save_summary(overall_stats)

        # Print final summary
        self.print_summary(overall_stats)

        return overall_stats

    def save_error_log(self) -> None:
        """Save errors to CSV file."""
        if not self.errors:
            logger.info("No errors to log")
            return

        try:
            df = pd.DataFrame(self.errors)
            df.to_csv(OCR_ERRORS_FILE, index=False)
            logger.info(f"Saved {len(self.errors)} errors to {OCR_ERRORS_FILE}")
        except Exception as e:
            logger.error(f"Failed to save error log: {e}")

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
        summary_path = DEEPSEEK_OCR_RESULTS_DIR / "_processing_summary.json"
        try:
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved processing summary to {summary_path}")
        except Exception as e:
            logger.error(f"Failed to save summary: {e}")

    def print_summary(self, stats: Dict[str, Any]) -> None:
        """Print final summary."""
        print("\n" + "=" * 70)
        print("DEEPSEEK OCR PIPELINE (VERTEX AI) COMPLETE")
        print("=" * 70)
        print(f"Total PDFs processed: {stats['processed_pdfs']}/{stats['total_pdfs']}")
        print(f"Total pages: {stats['total_pages']}")
        print(f"   Successful: {stats['successful_pages']}")
        print(f"      - Conventional extraction: {stats['conventional_pages']}")
        print(f"      - DeepSeek OCR: {stats['deepseek_ocr_pages']}")
        print(f"   Failed: {stats['failed_pages']}")
        print(f"\nSuccess rate: {stats['successful_pages']/stats['total_pages']*100:.1f}%")
        print(f"\nResults saved to: {DEEPSEEK_OCR_RESULTS_DIR}")
        if self.errors:
            print(f"Errors logged to: {OCR_ERRORS_FILE}")
        print("=" * 70 + "\n")


def main():
    """Main entry point for the DeepSeek OCR pipeline."""
    try:
        # Initialize pipeline
        logger.info("Initializing DeepSeek OCR Pipeline...")
        pipeline = DeepSeekOCRPipeline()

        # Test connection first
        if not pipeline.vertex_client.test_connection():
            logger.error("Failed to connect to Vertex AI. Check your credentials and configuration.")
            sys.exit(1)

        # Process all PDFs
        stats = pipeline.process_all_pdfs(cleanup_temp=True)

        # Exit with appropriate code
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
