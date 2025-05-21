import os
import cloudinary.uploader
import redis
from PIL import Image
import io
import requests
from dotenv import load_dotenv
from ..worker import celery_app

load_dotenv("../../.env")

# Configure Cloudinary
cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET"),
)

# Initialize Redis client
redis_client = redis.from_url(os.getenv("CELERY_BROKER_URL"), decode_responses=True)

@celery_app.task(name='image.compress')
def compress_image_task(task_id, file_url, quality=75):
    """Compress image task"""
    try:
        print(f"Starting image compression task {task_id}")
        
        # Update status to processing
        redis_client.hset(task_id, mapping={"status": "processing", "progress": "10"})
        
        # Download image from Cloudinary
        response = requests.get(file_url)
        response.raise_for_status()
        
        redis_client.hset(task_id, "progress", "30")
        
        # Open and compress image
        image = Image.open(io.BytesIO(response.content))
        
        # Convert to RGB if necessary (for JPEG)
        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")
        
        redis_client.hset(task_id, "progress", "50")
        
        # Compress image
        compressed_buffer = io.BytesIO()
        image.save(compressed_buffer, format="JPEG", quality=quality, optimize=True)
        compressed_buffer.seek(0)
        
        redis_client.hset(task_id, "progress", "70")
        
        # Upload compressed image to Cloudinary
        compressed_upload = cloudinary.uploader.upload(
            compressed_buffer.getvalue(),
            folder="mediaforge/compressed",
            format="jpg"
        )
        
        # Update progress and mark as completed
        redis_client.hset(task_id, mapping={
            "status": "completed",
            "progress": "100",
            "result_url": compressed_upload["secure_url"]
        })
        
        print(f"Completed image compression task {task_id}")
        return compressed_upload["secure_url"]
        
    except Exception as e:
        print(f"Error in image compression task {task_id}: {e}")
        redis_client.hset(task_id, mapping={
            "status": "failed",
            "error": str(e)
        })
        raise e