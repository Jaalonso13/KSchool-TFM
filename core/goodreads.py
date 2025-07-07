from __future__ import annotations

import os
import requests
import pandas as pd
from tqdm import tqdm

from core.config import DATA_PATH, DATA_FILE_NAMES, API_KEY, API_HOST


class GoodReadsData:
    def __init__(self, 
                 names_file: str = DATA_FILE_NAMES, 
                 data_path: str = DATA_PATH,
                 donwload_workers: int = 4):
        
        self._file_names = pd.read_csv(names_file).set_index("name")
        self._data_path = os.path.join(data_path, "goodreads")
        self._download_workers = donwload_workers
        
        self._url = "https://mcauleylab.ucsd.edu/public_datasets/gdrive/goodreads"
        
    @property
    def file_names(self) -> list[str]:
        return self._file_names.index.tolist()
        
    def download_all(self):
        for i, filename in enumerate(self.file_names):
            print(f"Downloading file {i+1}/{len(self.file_names)} -> {filename}")
            self.download_file(filename)
        
    def _byGenre(self, filename: str) -> bool:
        return self._file_names.loc[filename, "type"] == "byGenre"
            
    def _get_url(self, filename: str) -> str:
        return f"{self._url}{"/byGenre" if self._byGenre(filename) else ""}" \
            + f"/{filename}{self._file_names.loc[filename, "ext"]}"
            
    def get_file_path(self, filename: str) -> str:            
        return os.path.join(
            self._data_path, "byGenre" if self._byGenre(filename) else "", 
            self._file_names.loc[filename, "tag"], 
            f"{filename}{self._file_names.loc[filename, "ext"]}")
                    
    def download_file(self, filename: str):
        file_path = self.get_file_path(filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        try:
            with requests.get(self._get_url(filename), stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('Content-Length', 0))
                
                with open(file_path, 'wb') as f, tqdm(
                    total=total_size, unit='B', unit_scale=True, desc=filename
                ) as pbar:
                    
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        pbar.update(len(chunk))
                        
            print(f"File {filename} has been downloaded!")
            
        except Exception as err:
            print(f"File {filename} can not be found!")
            raise RuntimeError(err)
        
    def load_file(self, filename: str) -> pd.DataFrame | None:
        file_path = self.get_file_path(filename)
        try:
            if (ext := os.path.splitext(file_path)[-1]) == ".gz":
                return pd.read_json(file_path, lines=True, compression="gzip")
            elif ext == ".json":
                return pd.read_json(file_path)
            elif ext == ".csv":
                return pd.read_csv(file_path)
        except Exception as err:
            print(f"File {filename} can not be loaded!")
        return None


class GoodReadsApi:
    
    def __init__(self, key: str = API_KEY, host: str = API_HOST):
        self._headers = {"x-rapidapi-key": key, "x-rapidapi-host": host}
        self._url = "https://goodreads12.p.rapidapi.com"
        
    def call_api(self, endpoint: str, query: dict) -> dict | None:
        try:
            response = requests.get(
                self._url + f"/{endpoint}", headers=self._headers, params=query)
        except:
            print(f"Error while calling API (endpint={endpoint}, query={query})")
            return None
        return response.json()
    
    def get_book_by_id(self, _id: str) -> dict | None:
        query = {"bookID": _id}
        return self.call_api("getBookByID", query)
    
    def get_book_by_url(self, url: str) -> dict | None:
        query = {"url": url}
        return self.call_api("getBookByURL", query)
    
    def get_author_books(self, author_id: str) -> dict | None:
        query = {"authorID": author_id}
        return self.call_api("getAuthorBooks", query)