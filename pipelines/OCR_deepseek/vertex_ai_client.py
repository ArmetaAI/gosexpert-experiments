"""
Vertex AI Client for OCR using Gemini Vision
Uses Google Cloud Vertex AI with Gemini Vision models for OCR processing.
"""
import os
import json
import logging
import base64
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Import Vertex AI SDK
from google.cloud import aiplatform
from vertexai.preview.generative_models import GenerativeModel, Part, Image

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class VertexAIDeepSeekClient:
    """Client for OCR via Vertex AI using Gemini Vision models."""

    def __init__(
        self,
        project_id: Optional[str] = None,
        location: str = "us-central1",
        model_name: str = "gemini-1.5-flash-002"
    ):
        """
        Initialize Vertex AI client for OCR using Gemini Vision.

        Args:
            project_id: Google Cloud project ID (reads from env if not provided)
            location: Google Cloud region
            model_name: Gemini Vision model to use (gemini-1.5-flash-002 or gemini-1.5-pro-002)
        """
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.location = location
        self.model_name = model_name

        if not self.project_id:
            raise ValueError(
                "GOOGLE_CLOUD_PROJECT must be set in .env or passed as parameter"
            )

        # Initialize Vertex AI
        aiplatform.init(project=self.project_id, location=self.location)

        logger.info(f"Initialized Vertex AI client")
        logger.info(f"  Project: {self.project_id}")
        logger.info(f"  Location: {self.location}")
        logger.info(f"  Model: {self.model_name}")

        # OCR prompt for DeepSeek
        self.ocr_prompt = """Extract all text from this document image with high accuracy.

Analyze the document and provide:

1. **text**: Complete extracted text from the document, preserving layout and structure
2. **headings**: List of all headings/titles identified (font size, boldness, position)
3. **tables**: List of any tables found with their structure and content
4. **images**: List of images/figures with descriptions and captions
5. **structure**: Document structure analysis (has_headings, has_tables, has_lists, has_images, document_type)

Return the response as a valid JSON object with this structure:
{
  "text": "full extracted text here...",
  "headings": ["Heading 1", "Heading 2"],
  "tables": [
    {
      "id": "TABLE_1",
      "caption": "Table caption",
      "data": [["row1col1", "row1col2"], ["row2col1", "row2col2"]]
    }
  ],
  "images": [
    {
      "id": "IMAGE_1",
      "caption": "Image caption",
      "description": "Image description",
      "type": "chart/diagram/photo",
      "position": "top/middle/bottom"
    }
  ],
  "structure": {
    "has_headings": true,
    "has_tables": true,
    "has_lists": false,
    "has_images": true,
    "document_type": "report/article/form/letter/etc"
  }
}

Be thorough and accurate. Preserve formatting, special characters, and structure."""

    def encode_image(self, image_path: Path) -> str:
        """
        Encode image to base64 string.

        Args:
            image_path: Path to the image file

        Returns:
            Base64 encoded image string
        """
        with open(image_path, 'rb') as f:
            image_bytes = f.read()
        return base64.b64encode(image_bytes).decode('utf-8')

    def generate(
        self,
        image_path: Path,
        max_tokens: int = 8192,
        temperature: float = 0.0
    ) -> Dict[str, Any]:
        """
        Send image to DeepSeek via Vertex AI for OCR processing.

        Args:
            image_path: Path to the image file
            max_tokens: Maximum tokens for response
            temperature: Sampling temperature (0.0 = deterministic)

        Returns:
            Structured OCR result dictionary
        """
        logger.info(f"Processing image: {image_path.name}")

        try:
            # Load image
            image = Image.load_from_file(str(image_path))

            # Create model instance
            model = GenerativeModel(self.model_name)

            # Generate content
            response = model.generate_content(
                [self.ocr_prompt, image],
                generation_config={
                    "max_output_tokens": max_tokens,
                    "temperature": temperature,
                }
            )

            # Extract text response
            raw_response = response.text

            # Try to parse as JSON
            try:
                # Remove markdown code blocks if present
                cleaned_response = raw_response.strip()
                if cleaned_response.startswith("```json"):
                    cleaned_response = cleaned_response[7:]
                if cleaned_response.startswith("```"):
                    cleaned_response = cleaned_response[3:]
                if cleaned_response.endswith("```"):
                    cleaned_response = cleaned_response[:-3]
                cleaned_response = cleaned_response.strip()

                # Parse JSON
                parsed_result = json.loads(cleaned_response)

                # Ensure required fields
                result = {
                    'text': parsed_result.get('text', ''),
                    'headings': parsed_result.get('headings', []),
                    'tables': parsed_result.get('tables', []),
                    'images': parsed_result.get('images', []),
                    'structure': parsed_result.get('structure', {
                        'has_headings': False,
                        'has_tables': False,
                        'has_lists': False,
                        'has_images': False,
                        'document_type': 'unknown'
                    }),
                    'raw_response': raw_response
                }

                logger.info(f"  ✓ OCR successful - {len(result['text'])} chars extracted")
                return result

            except json.JSONDecodeError as e:
                logger.warning(f"  ⚠ Failed to parse JSON response: {e}")
                logger.warning("  Falling back to raw text extraction")

                # Fallback: return raw text
                result = {
                    'text': raw_response,
                    'headings': [],
                    'tables': [],
                    'images': [],
                    'structure': {
                        'has_headings': False,
                        'has_tables': False,
                        'has_lists': False,
                        'has_images': False,
                        'document_type': 'unknown'
                    },
                    'raw_response': raw_response
                }

                return result

        except Exception as e:
            logger.error(f"  ✗ OCR failed: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test connection to Vertex AI.

        Returns:
            True if connection is successful
        """
        try:
            # Try to list models to verify connection
            logger.info("Testing Vertex AI connection...")
            model = GenerativeModel(self.model_name)
            logger.info("✓ Connection successful")
            return True
        except Exception as e:
            logger.error(f"✗ Connection failed: {e}")
            return False
