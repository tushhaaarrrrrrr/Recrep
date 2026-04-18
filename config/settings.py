import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")
DIRECT_URL = os.getenv("DIRECT_URL")
OWNER_ID = os.getenv("OWNER_ID")

SUPABASE_ENDPOINT = os.getenv("SUPABASE_ENDPOINT")
SUPABASE_ACCESS_KEY_ID = os.getenv("SUPABASE_ACCESS_KEY_ID")
SUPABASE_SECRET_ACCESS_KEY = os.getenv("SUPABASE_SECRET_ACCESS_KEY")
SUPABASE_REGION = os.getenv("SUPABASE_REGION")
SUPABASE_BUCKET_NAME = os.getenv("SUPABASE_BUCKET_NAME")

REQUIRED_VARS = {
    "DISCORD_TOKEN": DISCORD_TOKEN,
    "DATABASE_URL": DATABASE_URL,
    "OWNER_ID": OWNER_ID,
    "SUPABASE_ENDPOINT": SUPABASE_ENDPOINT,
    "SUPABASE_ACCESS_KEY_ID": SUPABASE_ACCESS_KEY_ID,
    "SUPABASE_SECRET_ACCESS_KEY": SUPABASE_SECRET_ACCESS_KEY,
    "SUPABASE_REGION": SUPABASE_REGION,
    "SUPABASE_BUCKET_NAME": SUPABASE_BUCKET_NAME,
}

missing_vars = [name for name, value in REQUIRED_VARS.items() if not value]
if missing_vars:
    raise RuntimeError(
        f"Missing required environment variables: {', '.join(missing_vars)}\n"
        "Please check your .env file."
    )