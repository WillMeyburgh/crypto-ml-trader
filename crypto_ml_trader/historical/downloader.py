import asyncio
from datetime import datetime
from typing import List
from alive_progress import alive_bar

from crypto_ml_trader.config import Config
from crypto_ml_trader.historical.bulk import download_single, get_range_downloads


class Downloader:
    def __init__(
        self,
        symbols: List[str],
        start: datetime = None,
        end: datetime = None,
        tdtype: str = Config.default_tdtype(),
        trade: str = Config.default_trade(),
        interval: str = Config.default_interval(),
        batch_size: int = 5
    ):
        self.symbols = symbols
        self.start = start
        self.end = end
        self.tdtype = tdtype
        self.trade = trade
        self.interval = interval
        self.batch_size = batch_size

    async def run(self):
        all_downloads = []

        for s_i, symbol in enumerate(self.symbols):
            all_downloads.append(
                get_range_downloads(
                    symbol,
                    self.start,
                    self.end,
                    self.tdtype,
                    self.trade,
                    self.interval,
                    truncate=True
                )
            )

        all_downloads = await asyncio.gather(*all_downloads)
        
        for i, downloads in enumerate(all_downloads):
            if len(downloads) > 0:
                with alive_bar(len(downloads)) as download_bar:
                    download_bar.title(self.symbols[i])

                    while len(downloads) > 0:
                        batch = []
                        for i in range(min(len(downloads), self.batch_size)):
                            batch.append(downloads.pop(0))

                        results = await asyncio.gather(
                            *[
                                download_single(**kwargs)
                                for kwargs in batch
                            ]
                        )

                        for result in results:
                            download_bar()

