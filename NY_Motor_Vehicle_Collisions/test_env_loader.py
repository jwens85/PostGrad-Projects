# test_env_load.py
from dotenv import load_dotenv
import os

load_dotenv()

print("AWS_ACCESS_KEY_ID:", os.getenv("AWS_ACCESS_KEY_ID"))
print("AWS_DEFAULT_REGION:", os.getenv("AWS_DEFAULT_REGION"))