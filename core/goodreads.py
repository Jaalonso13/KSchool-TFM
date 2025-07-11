from __future__ import annotations

import os
import requests
import gzip
import json
import pandas as pd
from tqdm import tqdm

from core.config import DATA_PATH, DATA_FILE_NAMES, API_KEY, API_HOST


class GoodReadsData:
    def __init__(self, 
                 names_file: str = DATA_FILE_NAMES, 
                 data_path: str = DATA_PATH,
                 donwload_workers: int = 4):
        
        self._file_names: pd.DataFrame = pd.read_csv(names_file).set_index("name")
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
        _genre = "/byGenre" if self._byGenre(filename) else ""
        return f"{self._url}{_genre}" \
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
        
    def load_file(self, filename: str) -> pd.DataFrame:
        file_path = self.get_file_path(filename)
        try:
            if (ext := os.path.splitext(file_path)[-1]) == ".gz":
                return pd.read_json(file_path, lines=True, compression="gzip")
            elif ext == ".json":
                return pd.read_json(file_path, lines=True)
            elif ext == ".csv":
                return pd.read_csv(file_path)
            
        except Exception as err:
            print(f"File {filename} can not be loaded!")
            raise RuntimeError(err)
    
    def load_file_range(self, filename: str, sample_range: tuple[int, int]
                          ) -> pd.DataFrame:
        data = []
        with gzip.open(self.get_file_path(filename), "rt", encoding="utf-8") as fin:
            for i, l in enumerate(fin):
                if i < sample_range[0]:
                    continue
                elif i > sample_range[1]:
                    break
                data.append(json.loads(l))
        return pd.DataFrame(data)

    def load_file_columns(self, filename: str, columns: list[str]
                          ) -> pd.DataFrame:
        data = []
        with gzip.open(self.get_file_path(filename), "rt", encoding="utf-8") as fin:
            for l in fin:
                obj = json.loads(l)
                data.append({key: obj.get(key, None) for key in columns})
        return pd.DataFrame(data)
        
    def load_file_samples(self, filename: str, key: str, values: list
                         ) -> pd.DataFrame:
        data = []
        with gzip.open(self.get_file_path(filename), "rt", encoding="utf-8") as fin:
            for l in fin:
                obj = json.loads(l)
                if obj.get(key, None) in values:
                    data.append(obj)
        return pd.DataFrame(data)
    
    
    def count_samples(self, filename: str) -> int:
        count = 0
        with gzip.open(self.get_file_path(filename), 'rt', encoding='utf-8') as f:
            for _ in f:
                count += 1
        return count

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