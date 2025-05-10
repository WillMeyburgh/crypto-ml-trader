import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from crypto_ml_trader.config import Config
from crypto_ml_trader.dataset.historic_dataset import HistoricDataset
from crypto_ml_trader.historical.downloader import Downloader
from crypto_ml_trader.trader import Trader

class Simulator:
    def __init__(
        self,
        symbols: List[str],
        start: datetime = None,
        end: datetime = None,
        tdtype: str = Config.default_tdtype(),
        trade: str = Config.default_trade(),
        interval: str = Config.default_interval(),
    ):
        self.symbols = symbols
        self.start = start
        self.end = end
        self.tdtype = tdtype
        self.trade = trade
        self.interval = interval
        self.__datasets = None

    async def datasets(self) -> Dict[str, HistoricDataset]:
        if self.__datasets is None:
            downloader = Downloader(
                self.symbols,
                self.start,
                self.end,
                self.tdtype,
                self.trade,
                self.interval
            )

            await downloader.run()
            
            datasets = []

            for symbol in self.symbols:
                datasets.append(
                    HistoricDataset.range(
                        symbol,
                        self.start,
                        self.end,
                        self.tdtype,
                        self.trade,
                        self.interval
                    )
                )
            
            datasets = await asyncio.gather(*datasets)

            self.__datasets = {}
            for i in range(len(datasets)):
                self.__datasets[self.symbols[i]] = datasets[i]

        return self.__datasets

    async def simulate(self, trader: Trader):
        pass