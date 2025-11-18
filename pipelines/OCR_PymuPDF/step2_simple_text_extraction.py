"""
Step 3: Simple Text Extraction from PDFs
Extracts text from all PDFs in the downloaded_pdfs directory using PyMuPDF.
If a page has minimal/no text or extraction fails, that page is skipped.

Output:
- JSON files per PDF with structure: {"page1": "text", "page2": "text", ...}
- CSV tracking file with extraction statistics
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import fitz  # PyMuPDF
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('text_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent.parent
DOWNLOADED_PDFS_DIR = PROJECT_ROOT / "downloaded_pdfs"
TEXT_RESULTS_DIR = PROJECT_ROOT / "text_extraction_results"
TRACKING_CSV = PROJECT_ROOT / "text_extraction_tracking.csv"

# Minimum characters threshold to consider page as having valid text
MIN_CHARS = 10


class SimpleTextExtractor:
    """Extracts text from PDFs and tracks results."""

    def __init__(self):
        """Initialize the text extractor."""
        self.tracking_data: List[Dict[str, Any]] = []

        # Create output directory
        TEXT_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

        logger.info("Simple Text Extractor initialized")
        logger.info(f"PDFs directory: {DOWNLOADED_PDFS_DIR}")
        logger.info(f"Results directory: {TEXT_RESULTS_DIR}")

    def extract_text_from_pdf(self, pdf_path: Path) -> Dict[str, Any]:
        """
        Extract text from all pages of a PDF.

        Args:
            pdf_path: Path to the PDF file

        Returns:
            Dict with extraction results and statistics
        """
        logger.info(f"Processing: {pdf_path.name}")

        pdf_name = pdf_path.stem
        page_texts = {}
        total_pages = 0
        extracted_pages = 0
        unextractable_pages = 0
        error_pages = 0

        try:
            # Open PDF
            pdf_document = fitz.open(pdf_path)
            total_pages = len(pdf_document)

            # Extract text from each page
            for page_num in range(total_pages):
                page_key = f"page{page_num + 1}"

                try:
                    page = pdf_document[page_num]
                    text = page.get_text().strip()

                    # Check if text meets minimum threshold
                    if len(text) >= MIN_CHARS:
                        page_texts[page_key] = text
                        extracted_pages += 1
                        logger.debug(f"  {page_key}: Extracted {len(text)} chars")
                    else:
                        unextractable_pages += 1
                        logger.debug(f"  {page_key}: Skipped (insufficient text: {len(text)} chars)")

                except Exception as e:
                    error_pages += 1
                    logger.warning(f"  {page_key}: Error - {e}")

            pdf_document.close()

        except Exception as e:
            logger.error(f"Failed to open PDF {pdf_path.name}: {e}")
            return {
                'filename': pdf_name,
                'total_pages': 0,
                'extracted': 0,
                'unextractable': 0,
                'errors': 1,
                'has_results': False,
                'error_message': str(e)
            }

        # Determine if we got any usable results
        has_results = len(page_texts) > 0

        # Save results if we have any
        if has_results:
            self.save_json_result(pdf_name, page_texts)
        else:
            logger.warning(f"No extractable text found in {pdf_path.name}")

        # Build statistics
        stats = {
            'filename': pdf_name,
            'total_pages': total_pages,
            'extracted': extracted_pages,
            'unextractable': unextractable_pages,
            'errors': error_pages,
            'has_results': has_results,
            'processed_at': datetime.now().isoformat()
        }

        logger.info(f"  Complete: {extracted_pages}/{total_pages} pages extracted")

        return stats

    def save_json_result(self, pdf_name: str, page_texts: Dict[str, str]) -> None:
        """
        Save extracted text to JSON file.

        Args:
            pdf_name: PDF filename without extension
            page_texts: Dictionary mapping page numbers to text
        """
        output_path = TEXT_RESULTS_DIR / f"{pdf_name}.json"

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(page_texts, f, indent=2, ensure_ascii=False)

            logger.info(f"  Saved: {output_path.name}")

        except Exception as e:
            logger.error(f"  Failed to save JSON for {pdf_name}: {e}")

    def process_all_pdfs(self) -> None:
        """Process all PDFs in the downloaded_pdfs directory."""
        logger.info(f"\n{'='*70}")
        logger.info("Starting Text Extraction for All PDFs")
        logger.info(f"{'='*70}\n")

        # Get all PDF files
        pdf_files = sorted(DOWNLOADED_PDFS_DIR.glob("*.pdf"))

        if not pdf_files:
            logger.warning(f"No PDF files found in {DOWNLOADED_PDFS_DIR}")
            return

        logger.info(f"Found {len(pdf_files)} PDF files\n")

        # Process each PDF
        for idx, pdf_path in enumerate(pdf_files, 1):
            logger.info(f"[{idx}/{len(pdf_files)}] {pdf_path.name}")

            stats = self.extract_text_from_pdf(pdf_path)
            self.tracking_data.append(stats)

            logger.info("")  # Blank line for readability

        # Save tracking CSV
        self.save_tracking_csv()

        # Print summary
        self.print_summary()

    def save_tracking_csv(self) -> None:
        """Save tracking data to CSV file."""
        if not self.tracking_data:
            logger.warning("No tracking data to save")
            return

        try:
            df = pd.DataFrame(self.tracking_data)
            df.to_csv(TRACKING_CSV, index=False)
            logger.info(f"Saved tracking data to: {TRACKING_CSV}")
        except Exception as e:
            logger.error(f"Failed to save tracking CSV: {e}")

    def print_summary(self) -> None:
        """Print final summary statistics."""
        if not self.tracking_data:
            return

        total_pdfs = len(self.tracking_data)
        successful = sum(1 for item in self.tracking_data if item['has_results'])
        total_pages = sum(item['total_pages'] for item in self.tracking_data)
        extracted_pages = sum(item['extracted'] for item in self.tracking_data)
        unextractable_pages = sum(item['unextractable'] for item in self.tracking_data)
        error_pages = sum(item['errors'] for item in self.tracking_data)

        print("\n" + "=" * 70)
        print("TEXT EXTRACTION COMPLETE")
        print("=" * 70)
        print(f"PDFs processed: {total_pdfs}")
        print(f"  With extractable text: {successful}")
        print(f"  Without extractable text: {total_pdfs - successful}")
        print(f"\nPages:")
        print(f"  Total: {total_pages}")
        print(f"  Extracted: {extracted_pages}")
        print(f"  Unextractable: {unextractable_pages}")
        print(f"  Errors: {error_pages}")
        if total_pages > 0:
            print(f"\nSuccess rate: {extracted_pages/total_pages*100:.1f}%")
        print(f"\nResults saved to: {TEXT_RESULTS_DIR}")
        print(f"Tracking file: {TRACKING_CSV}")
        print("=" * 70 + "\n")


def main():
    """Main entry point."""
    try:
        extractor = SimpleTextExtractor()
        extractor.process_all_pdfs()
        logger.info("Text extraction completed successfully")

    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user")
    except Exception as e:
        logger.error(f"Text extraction failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()
