import boto3
import uuid
import asyncio
from io import BytesIO
from botocore.exceptions import ClientError
from config.settings import (
    SUPABASE_ENDPOINT, SUPABASE_ACCESS_KEY_ID, SUPABASE_SECRET_ACCESS_KEY,
    SUPABASE_REGION, SUPABASE_BUCKET_NAME
)
from utils.logger import get_logger

logger = get_logger(__name__)

_s3_client = None


def init_s3_client():
    """Initialize the Supabase S3 client (S3-compatible)."""
    global _s3_client
    try:
        config = boto3.session.Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )
        _s3_client = boto3.client(
            's3',
            endpoint_url=SUPABASE_ENDPOINT,
            aws_access_key_id=SUPABASE_ACCESS_KEY_ID,
            aws_secret_access_key=SUPABASE_SECRET_ACCESS_KEY,
            region_name=SUPABASE_REGION,
            config=config,
        )
        logger.info(f"Supabase S3 client initialized. Bucket: {SUPABASE_BUCKET_NAME}")
        return _s3_client
    except Exception as e:
        logger.exception("Failed to initialize Supabase S3 client")
        raise


async def upload_image(file_bytes: bytes, filename: str) -> str:
    """
    Upload an image to Supabase Storage S3 and return the public URL.

    This function runs the blocking upload in a thread pool to avoid
    stalling the asyncio event loop.
    """
    if _s3_client is None:
        raise RuntimeError("S3 client not initialized. Call init_s3_client() first.")

    # Determine file extension
    ext = filename.split('.')[-1].lower()
    if ext not in ('png', 'jpg', 'jpeg', 'gif', 'webp'):
        ext = 'png'

    key = f"uploads/{uuid.uuid4()}.{ext}"
    file_obj = BytesIO(file_bytes)

    # Offload the blocking upload to a thread
    def _do_upload():
        _s3_client.upload_fileobj(
            file_obj,
            SUPABASE_BUCKET_NAME,
            key,
            ExtraArgs={'ContentType': f'image/{ext}'}
        )

    try:
        await asyncio.to_thread(_do_upload)
    except ClientError as e:
        logger.exception("Supabase S3 upload failed")
        raise RuntimeError(f"Upload failed: {e.response['Error']['Message']}")
    except Exception as e:
        logger.exception("Unexpected error during upload")
        raise

    # Build public URL
    base_url = SUPABASE_ENDPOINT.replace('/storage/v1/s3', '')
    url = f"{base_url}/storage/v1/object/public/{SUPABASE_BUCKET_NAME}/{key}"

    logger.debug(f"Uploaded image: {url}")
    return url


async def delete_image(url: str):
    """
    Delete an image from S3 given its public URL.
    """
    if _s3_client is None:
        raise RuntimeError("S3 client not initialized. Call init_s3_client() first.")
    try:
        key = url.split(f"/object/public/{SUPABASE_BUCKET_NAME}/")[-1]
        await asyncio.to_thread(
            _s3_client.delete_object,
            Bucket=SUPABASE_BUCKET_NAME,
            Key=key
        )
        logger.debug(f"Deleted S3 object: {key}")
    except Exception as e:
        logger.error(f"Failed to delete {url}: {e}")
        raise