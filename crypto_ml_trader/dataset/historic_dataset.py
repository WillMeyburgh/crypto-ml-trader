from dataclasses import asdict, dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import List
from torch.utils.data import Dataset

from crypto_ml_trader.config import Config
from crypto_ml_trader.historical.bulk import download_range
import pandas as pd
import torch
import numpy as np

@dataclass
class FileCache:
    length: int

    @classmethod
    def cache_file(cls, file: Path) -> Path:
        return file.parent / f'.cache.{file.name}'

    @classmethod
    def new(cls, file: Path) -> "FileCache":
        cache_file = cls.cache_file(file)

        if cache_file.exists():
            return cls.load(cache_file)
        
        df = pd.read_csv(file, header=None)

        result = FileCache(
            length=df.shape[0]
        )

        result.save(cache_file)

        return result

    @classmethod
    def load(cls, cache_file: Path) -> "FileCache":
        with open(cache_file, 'r') as f:
            return FileCache(**json.load(f))
        
    def save(self, cache_file: Path):
        with open(cache_file, 'w') as f:
            json.dump(asdict(self), f)

class HistoricDataset(Dataset):
    def __init__(
        self, 
        files: List[Path], 
        start: datetime, 
        end: datetime
    ):
        super().__init__()

        self.files = files
        self.start = start
        self.end = end

        self.df = None

        self.initial_indexs()
        
    def initial_indexs(self):
        self.start_index = None # relative index where to start from on the first file (inclusive)
        self.end_index = None # relative index where to end on the last file (exclusive)
        self.offset_indexs = [] # absolute indexs to make make absolute index relative to file
        self.file_index = -1 # current index of the loaded `self.df`
        self.relative_index = -1 # current index of the item relative to the current `self.df`
        self.length = -1 # length from first files start_index to last files end_index
        self.last_item_index = -999 # last item index retrieved


        self.load_df(len(self.files)-1)
        if self.end is None:
            self.end_index = self.df.shape[0]
        else:
            for i in range(self.df.shape[0]-1, -1, -1):
                timestamp = datetime.fromtimestamp(self.df.iloc[i, 0] / 1000)

                if timestamp <= self.end:
                    self.end_index = i + 1
                    break
                

        if self.start is None:
            self.start_index = 0
        else:
            self.load_df(0)
            for i in range(self.df.shape[0]):
                timestamp = datetime.fromtimestamp(self.df.iloc[i, 0] / 1000)
                if self.start <= timestamp:
                    self.start_index = i
                    break

        offset_index = 0
        for i in range(len(self.files)):
            self.offset_indexs.append(offset_index)
            offset_index += FileCache.new(self.files[i]).length

            if i == 0:
                offset_index -= self.start_index

        
        self.length = self.offset_indexs[-1] + self.end_index
        self.offset_indexs.append(self.length)


    def item_to_file_index(self, item_index):
        # optimized for sequancal read
        if item_index == self.last_item_index + 1:
            if item_index < self.offset_indexs[self.file_index+1]:
                return self.file_index
            return self.file_index + 1
        
        for file_index, offset_index in enumerate(self.offset_indexs):
            if offset_index > item_index:
                return file_index-1
            
        return None


    def item_to_relative_index(self, file_index, item_index):
        # optimized for sequancal read
        if item_index == self.last_item_index + 1:
            if item_index < self.offset_indexs[file_index+1]:
                return self.relative_index + 1
            return 0
        
        relative_index = item_index - self.offset_indexs[file_index]
        if file_index == 0:
            relative_index += self.start_index

        return relative_index

    def load_df(self, file_index, relative_index=-1):
        if file_index != self.file_index:
            self.file_index = file_index
            self.relative_index = -1
            self.df = pd.read_csv(self.files[file_index], header=None)

            if self.df.iloc[0, 0] > 1e14:
                self.df.iloc[:, 0] = self.df.iloc[:, 0] // 1000
        
        if relative_index != -1:
            self.relative_index = relative_index

    def parse_row(self, row: pd.Series):
        return torch.from_numpy(row.to_numpy())

    def __len__(self):
        return self.length
    
    def __getitem__(self, item_index):
        if item_index < 0:
            item_index = self.length + item_index

        file_index = self.item_to_file_index(item_index)
        relative_index = self.item_to_relative_index(file_index, item_index)

        self.last_item_index = item_index
        self.load_df(file_index, relative_index)
        return self.parse_row(self.df.iloc[relative_index, :])

    @classmethod
    async def range(
        cls,
        symbol: str,
        start: datetime,
        end: datetime,
        tdtype: str = Config.default_tdtype(),
        trade: str = Config.default_trade(),
        interval: str = Config.default_interval(),
    ):
        return cls(
            await download_range(
                symbol,
                start,
                end,
                tdtype,
                trade,
                interval
            ),
            start,
            end
        )

    @classmethod
    async def all(
        cls,
        symbol,
        tdtype: str = Config.default_tdtype(),
        trade: str = Config.default_trade(),
        interval: str = Config.default_interval(),
    ):
        return await cls.range(symbol, None, None, tdtype=tdtype, trade=trade, interval=interval)