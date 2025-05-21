import os
import cloudinary.uploader
import redis
import io
import requests
from dotenv import load_dotenv
from ..worker import celery_app
try:
    import fitz  # PyMuPDF
except ImportError:
    print("PyMuPDF not installed. Install with: pip install PyMuPDF")

load_dotenv("../../.env")

# Configure Cloudinary
cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET"),
)

redis_client = redis.from_url(os.getenv("CELERY_BROKER_URL"), decode_responses=True)

@celery_app.task(name='pdf.extract')
def extract_pdf_pages_task(task_id, file_url, start_page, end_page):
    """Extract pages from PDF task"""
    try:
        print(f"Starting PDF extraction task {task_id}")
        
        redis_client.hset(task_id, mapping={"status": "processing", "progress": "10"})
        
        # Download PDF
        response = requests.get(file_url)
        response.raise_for_status()
        
        redis_client.hset(task_id, "progress", "30")
        
        # Open PDF
        pdf_document = fitz.open(stream=response.content, filetype="pdf")
        
        # Validate page numbers (PyMuPDF uses 0-based indexing)
        total_pages = len(pdf_document)
        if start_page < 1 or end_page > total_pages or start_page > end_page:
            raise ValueError(f"Invalid page range. PDF has {total_pages} pages. "
                           f"Requested: {start_page}-{end_page}")
        
        redis_client.hset(task_id, "progress", "50")
        
        # Create new PDF for extracted pages
        extracted_pdf = fitz.open()
        
        # Extract pages (convert to 0-based indexing)
        start_idx = start_page - 1
        end_idx = end_page - 1
        
        for page_num in range(start_idx, end_idx + 1):
            extracted_pdf.insert_pdf(pdf_document, from_page=page_num, to_page=page_num)
        
        pdf_document.close()
        
        redis_client.hset(task_id, "progress", "75")
        
        # Save extracted PDF
        extracted_buffer = io.BytesIO()
        extracted_pdf.save(extracted_buffer)
        extracted_buffer.seek(0)
        
        extracted_pdf.close()
        
        redis_client.hset(task_id, "progress", "90")
        
        # Upload extracted PDF
        extracted_upload = cloudinary.uploader.upload(
            extracted_buffer.getvalue(),
            folder="mediaforge/pdf_extracted",
            resource_type="raw",
            format="pdf"
        )
        
        redis_client.hset(task_id, mapping={
            "status": "completed",
            "progress": "100",
            "result_url": extracted_upload["secure_url"],
            "extracted_pages": f"{start_page}-{end_page}"
        })
        
        print(f"Completed PDF extraction task {task_id}")
        return extracted_upload["secure_url"]
        
    except Exception as e:
        print(f"Error in PDF extraction task {task_id}: {e}")
        redis_client.hset(task_id, mapping={
            "status": "failed",
            "error": str(e)
        })
        raise e