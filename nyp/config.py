import os

import dotenv

dotenv.load_dotenv()

APP_SECRET = os.getenv("APP_SECRET")
LOCAL_RAW_DATA_FILE = os.getenv("LOCAL_RAW_DATA_FILE")
MYSQL_CON = os.getenv("MYSQL_CON")
