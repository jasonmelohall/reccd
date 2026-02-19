#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import boto3
import requests
import logging
from io import BytesIO
from PIL import Image
from config import get_settings
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
settings = get_settings()


class ImageService:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region
        )
        self.bucket_name = settings.s3_bucket_name
    
    def upload_image_from_url(self, image_url: str, asin: str) -> str:
        """
        Download image from URL and upload to S3
        
        Args:
            image_url: URL of the image to download
            asin: Product ASIN (used as filename)
            
        Returns:
            S3 URL of uploaded image
        """
        try:
            # Download image
            response = requests.get(image_url, timeout=10)
            response.raise_for_status()
            
            # Open and potentially resize image
            img = Image.open(BytesIO(response.content))
            
            # Convert to RGB if necessary (handles RGBA, P mode images)
            if img.mode in ('RGBA', 'P'):
                img = img.convert('RGB')
            
            # Resize if too large (max 800px on longest side)
            max_size = 800
            if max(img.size) > max_size:
                ratio = max_size / max(img.size)
                new_size = tuple(int(dim * ratio) for dim in img.size)
                img = img.resize(new_size, Image.Resampling.LANCZOS)
            
            # Save to BytesIO buffer
            buffer = BytesIO()
            img.save(buffer, format='JPEG', quality=85, optimize=True)
            buffer.seek(0)
            
            # Upload to S3
            s3_key = f"products/{asin}.jpg"
            self.s3_client.upload_fileobj(
                buffer,
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'ContentType': 'image/jpeg',
                    'CacheControl': 'max-age=31536000'  # 1 year cache
                }
            )
            
            # Generate public URL
            s3_url = f"https://{self.bucket_name}.s3.{settings.aws_region}.amazonaws.com/{s3_key}"
            logger.info(f"Successfully uploaded image for ASIN {asin} to S3")
            return s3_url
            
        except requests.RequestException as e:
            logger.error(f"Failed to download image from {image_url}: {e}")
            return None
        except ClientError as e:
            logger.error(f"Failed to upload image to S3: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing image for ASIN {asin}: {e}")
            return None
    
    def get_image_url(self, asin: str) -> str:
        """
        Get S3 URL for an ASIN's image
        
        Args:
            asin: Product ASIN
            
        Returns:
            S3 URL of image
        """
        s3_key = f"products/{asin}.jpg"
        
        # Check if image exists in S3
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return f"https://{self.bucket_name}.s3.{settings.aws_region}.amazonaws.com/{s3_key}"
        except ClientError:
            return None
    
    def image_exists(self, asin: str) -> bool:
        """Check if image exists in S3"""
        s3_key = f"products/{asin}.jpg"
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False


# Global instance
image_service = ImageService()



