import hashlib
import boto3
import pandas as pd

from botocore.client import Config
from tools import get_snakecase_row
from app_logger import get_logger

logger = get_logger(__name__)

class S3Client:
    def __init__(self, config):
        self.client = boto3.client("s3", **config, config=Config(signature_version="s3v4"))
        self.cache = {}

    def get_latest_object_key(self, bucket, prefix):
        objects = self.client.list_objects_v2(Bucket=bucket, Prefix=f"data/{prefix}")
        if "Contents" in objects:
            return max(objects["Contents"], key=lambda x: x["LastModified"])["Key"]
        return None

    def get_dataframe(self, bucket, key):
        cache_key = f"{bucket}/{key}"
        if cache_key in self.cache:
            logger.info(f"Использовано из кэша: {cache_key}")
            return self.cache[cache_key]

        try:
            obj = self.client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(obj["Body"], low_memory=False)                        
            df.columns = [get_snakecase_row(col) for col in df.columns]            
            self.cache[cache_key] = df
            return df
        except Exception as e:
            logger.error(f"Ошибка чтения CSV: {e}")
            return pd.DataFrame()

    def clear_cache(self):
        self.cache.clear()
