from __future__ import annotations

import os
from dotenv import load_dotenv

from core.utils import read_json_file

config: dict = read_json_file(os.path.abspath("./config/config.json"))

DATA_FILE_NAMES = os.path.abspath(config["data_file_names"])
DATA_PATH = os.path.abspath(config["data_path"])

load_dotenv()

API_KEY = os.getenv("API_KEY", None)
API_HOST = os.getenv("API_HOST", None)