import requests
from PIL import Image
from io import BytesIO
import time
from typing import Optional, Dict
import hashlib
import re
from supabase import create_client
from config import SUPABASE_URL, SUPABASE_KEY


def get_storage_bucket():
    """Get or create the storage bucket for compressed images"""
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    try:
        bucket = client.storage.get_bucket('compressed-images')
        return client, bucket
    except:
        try:
            bucket = client.storage.create_bucket('compressed-images', public=True)
            return client, bucket
        except Exception as e:
            print(f"  Could not create bucket: {e}")
            return client, None


def compress_and_upload(image_url: str, quality: int = 85, max_width: int = 800, 
                       bucket_name: str = 'product-images', folder: str = 'compressed') -> Optional[str]:
    """
    Compress image locally and upload to Supabase Storage.
    Returns the public URL of the compressed image.
    """
    if not image_url:
        return None
    
    try:
        response = requests.get(image_url, timeout=30)
        response.raise_for_status()
        original_size = len(response.content)
        
        img = Image.open(BytesIO(response.content))
        
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        
        if img.width > max_width:
            ratio = max_width / img.width
            new_height = int(img.height * ratio)
            img = img.resize((max_width, new_height), Image.LANCZOS)
        
        output = BytesIO()
        img.save(output, format='JPEG', quality=quality, optimize=True)
        compressed_bytes = output.getvalue()
        compressed_size = len(compressed_bytes)
        
        if compressed_size >= original_size:
            print(f"  Compression didn't help, using original")
            return image_url
        
        print(f"  Compressed: {original_size} -> {compressed_size} bytes ({(1 - compressed_size/original_size)*100:.1f}% reduction)")
        
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        filename = f"{folder}/{get_image_hash(image_url)}.jpg"
        
        try:
            response = client.storage.from_(bucket_name).upload(
                filename,
                compressed_bytes,
                {"content-type": "image/jpeg", "upsert": "true"}
            )
            
            path = getattr(response, 'path', None) or getattr(response, 'fullPath', None) or filename
            public_url = client.storage.from_(bucket_name).get_public_url(path)
            return public_url
        except Exception as e:
            print(f"  Upload failed: {e}")
        
        return image_url
        
    except Exception as e:
        print(f"  Error: {e}")
        return None


def compress_image(image_url: str, quality: int = 85, max_width: int = 800) -> Optional[Dict]:
    """
    Compress image locally - returns compression stats.
    """
    if not image_url:
        return None
    
    try:
        response = requests.get(image_url, timeout=30)
        response.raise_for_status()
        original_size = len(response.content)
        
        img = Image.open(BytesIO(response.content))
        
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        
        if img.width > max_width:
            ratio = max_width / img.width
            new_height = int(img.height * ratio)
            img = img.resize((max_width, new_height), Image.LANCZOS)
        
        output = BytesIO()
        img.save(output, format='JPEG', quality=quality, optimize=True)
        compressed_bytes = output.getvalue()
        compressed_size = len(compressed_bytes)
        
        return {
            'compressed_bytes': compressed_bytes,
            'original_size': original_size,
            'compressed_size': compressed_size,
            'percent': (1 - compressed_size / original_size) * 100 if original_size > 0 else 0,
        }
        
    except Exception as e:
        print(f"  Error: {e}")
        return None


def get_image_hash(image_url: str) -> str:
    """Generate a short hash from image URL for filename"""
    return hashlib.md5(image_url.encode()).hexdigest()[:12]


def get_compressed_url(image_url: str, quality: int = 85) -> str:
    """
    Compress and upload image to Supabase Storage.
    Returns the compressed URL or original if failed.
    """
    if not image_url:
        return image_url
    
    return compress_and_upload(image_url, quality)
