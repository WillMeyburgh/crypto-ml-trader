import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    @staticmethod
    def data_directory() -> Path:
        return Path(os.environ.get('DATA_DIRECTORY', 'data'))
    
    @staticmethod
    def default_trade() -> str:
        return os.environ.get('DEFAULT_TRADE', 'spot')
        
    @staticmethod
    def default_interval() -> str:
        return os.environ.get('DEFAULT_INTERVAL', '1m')
        
    @staticmethod
    def default_tdtype() -> str:
        return os.environ.get('DEFAULT_TRADE_DATATYPE', 'klines')

