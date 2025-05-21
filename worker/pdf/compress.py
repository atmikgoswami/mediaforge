import os
import cloudinary.uploader
import redis
import io
import requests
import tempfile
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

@celery_app.task(name='pdf.compress')
def compress_pdf_task(task_id, file_url, compression_level="medium"):
    """Compress PDF task"""
    try:
        print(f"Starting PDF compression task {task_id}")
        
        redis_client.hset(task_id, mapping={"status": "processing", "progress": "10"})
        
        # Download PDF
        response = requests.get(file_url)
        response.raise_for_status()
        
        redis_client.hset(task_id, "progress", "30")
        
        # Open PDF with PyMuPDF
        pdf_document = fitz.open(stream=response.content, filetype="pdf")
        
        redis_client.hset(task_id, "progress", "50")
        
        # Compression settings based on level
        compression_settings = {
            "low": {"deflate": 1, "deflate_images": True, "deflate_fonts": True},
            "medium": {"deflate": 6, "deflate_images": True, "deflate_fonts": True},
            "high": {"deflate": 9, "deflate_images": True, "deflate_fonts": True, "garbage": 4}
        }
        
        settings = compression_settings.get(compression_level, compression_settings["medium"])
        
        redis_client.hset(task_id, "progress", "70")
        
        # Save compressed PDF
        compressed_buffer = io.BytesIO()
        pdf_document.save(compressed_buffer, **settings)
        compressed_buffer.seek(0)
        
        pdf_document.close()
        
        redis_client.hset(task_id, "progress", "85")
        
        # Upload compressed PDF
        compressed_upload = cloudinary.uploader.upload(
            compressed_buffer.getvalue(),
            folder="mediaforge/pdf_compressed",
            resource_type="raw",
            format="pdf",
        )
        
        redis_client.hset(task_id, mapping={
            "status": "completed",
            "progress": "100",
            "result_url": compressed_upload["secure_url"]
        })
        
        print(f"Completed PDF compression task {task_id}")
        return compressed_upload["secure_url"]
        
    except Exception as e:
        print(f"Error in PDF compression task {task_id}: {e}")
        redis_client.hset(task_id, mapping={
            "status": "failed",
            "error": str(e)
        })
        raise e