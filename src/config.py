import os

from dotenv import load_dotenv

load_dotenv(".env")

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
