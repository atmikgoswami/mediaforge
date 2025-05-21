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

redis_client = redis.from_url(os.getenv("CELERY_BROKER_URL"), decode_responses=True)

@celery_app.task(name='image.convert')
def convert_image_task(task_id, file_url, target_format="PNG"):
    """Convert image format task"""
    try:
        print(f"Starting image conversion task {task_id}")
        
        redis_client.hset(task_id, mapping={"status": "processing", "progress": "10"})
        
        # Download image
        response = requests.get(file_url)
        response.raise_for_status()
        
        redis_client.hset(task_id, "progress", "30")
        
        # Open image
        image = Image.open(io.BytesIO(response.content))
        
        # Handle different format conversions
        if target_format.upper() == "JPEG" and image.mode in ("RGBA", "P"):
            image = image.convert("RGB")
        elif target_format.upper() == "PNG" and image.mode != "RGBA":
            image = image.convert("RGBA")
        
        redis_client.hset(task_id, "progress", "60")
        
        # Convert image
        converted_buffer = io.BytesIO()
        save_kwargs = {"format": target_format.upper()}
        
        if target_format.upper() == "JPEG":
            save_kwargs["quality"] = 95
            save_kwargs["optimize"] = True
        
        image.save(converted_buffer, **save_kwargs)
        converted_buffer.seek(0)
        
        redis_client.hset(task_id, "progress", "80")
        
        # Upload converted image
        converted_upload = cloudinary.uploader.upload(
            converted_buffer.getvalue(),
            folder="mediaforge/converted",
            format=target_format.lower()
        )
        
        redis_client.hset(task_id, mapping={
            "status": "completed",
            "progress": "100",
            "result_url": converted_upload["secure_url"]
        })
        
        print(f"Completed image conversion task {task_id}")
        return converted_upload["secure_url"]
        
    except Exception as e:
        print(f"Error in image conversion task {task_id}: {e}")
        redis_client.hset(task_id, mapping={
            "status": "failed",
            "error": str(e)
        })
        raise e