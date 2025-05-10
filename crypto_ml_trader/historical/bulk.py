import asyncio
from datetime import datetime, timedelta, time
from pathlib import Path
import re
from typing import Any, Dict, List
import bs4

import aiohttp
from crypto_ml_trader.config import Config
from crypto_ml_trader.utils.http import download_url
import zipfile

from bs4 import XMLParsedAsHTMLWarning
import warnings

import pandas as pd

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

__DOWNLOAD_KLINES_DIRECTORY = Config.data_directory() / 'historical/bulk/klines'
__DOWNLOAD_KLINES_URL = 'https://data.binance.vision/data/{trade}/{period}/klines/{symbol}/{interval}/{symbol}-{interval}-{date}.zip'
__DOWNLOAD_KLINES_FILENAME = '{symbol}-{interval}-{date}.csv'
__SYMBOL_DATES_URL = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/?list-type=2&prefix=data/{trade}/{period}/klines/{symbol}/{interval}/'

def __klines_single_file(
    symbol,
    date,
    trade,
    period,
    interval
) -> Path:
    directory: Path = __DOWNLOAD_KLINES_DIRECTORY / trade / period / symbol / interval
    directory.mkdir(parents=True, exist_ok=True)
    return directory / __DOWNLOAD_KLINES_FILENAME.format(
        symbol=symbol,
        interval=interval,
        date=date
    )


async def __download_klines_single(
    symbol,
    date,
    trade,
    period,
    interval
) -> Path:
    """
    downloads a single klines file from binance servers,
    will only download if not avaiable already
    """
    url = __DOWNLOAD_KLINES_URL.format(
        symbol=symbol,
        date=date,
        trade=trade,
        period=period,
        interval=interval
    )
    destination = __klines_single_file(
        symbol=symbol,
        date=date,
        trade=trade,
        period=period,
        interval=interval

    )
    if destination.exists():
        return destination

    tmp_destination = destination.parent / f'{destination.name[:-4]}.tmp.zip'
    
    await download_url(url, tmp_destination)

    with zipfile.ZipFile(tmp_destination, 'r') as zip:
        zip.extractall(destination.parent)

    tmp_destination.unlink()

    return destination

async def __symbol_dates(
    symbol,
    trade,
    period,
    interval
) -> List[str]:
    dates = []

    now = datetime.now()
    stop_date = f'{now.year}-{now.month-1:02}'
    if period == 'daily':
        stop_date = f'{now.year}-{now.month:02}-{now.day-1:02}'
    start_after = None

    async with aiohttp.ClientSession() as session:
        while True:
            url = __SYMBOL_DATES_URL.format(
                symbol=symbol,
                trade=trade,
                period=period,
                interval=interval
            )
            if start_after:
                url = url + f'&start-after={start_after}'

            async with session.get(url) as response:
                start_len = len(dates)

                soup = bs4.BeautifulSoup(await response.text(), "lxml")
                keys = soup.find_all("key")

                if len(keys) == 1:
                    stop_date = start_after[start_after.rindex('/')+1:]
                    stop_date = stop_date[len(symbol)+len(interval)+2:stop_date.index('.')]
                    break

                for key in keys:
                    if not key.text.endswith(".CHECKSUM"):
                        start_after = key.text
                        date = key.text[key.text.rindex('/')+1:]
                        date = date[len(symbol)+len(interval)+2:date.index('.')]
                        dates.append(date)

                if len(dates) == 0:
                    break

                if dates[-1] == stop_date:
                    break

                if start_len == len(dates):
                    print("error - no new keys, but still looping")

    if dates[-1] != stop_date:
        raise Exception("Incomplete call")
    
    return dates

async def __get_klines_range_downloads(
    symbol: str,
    start: datetime,
    end: datetime,
    trade: str,
    interval: str,
    truncate: bool = False
) -> List[Dict[str, Any]]:
    now = datetime.now()
    yesterday = datetime.combine((now - timedelta(days=1)), time.max)

    if (start and start > yesterday) or (end and end > yesterday):
        raise NotImplementedError("Download of today's data is not implemented")

    monthly_dates = await __symbol_dates(
        symbol,
        trade,
        "monthly",
        interval
    )

    start_date_index = 0 if start is None else -1
    end_date_index = -1

    for i, monthly_date in enumerate(monthly_dates):
        year = int(monthly_date[:4])
        month = int(monthly_date[-2:])

        if start and start.year == year and start.month == month:
            start_date_index = i

        if end and end.year == year and end.month == month:
            end_date_index = i

    downloads = []

    if start_date_index != -1:
        end_index = len(monthly_dates) if end_date_index == -1 else end_date_index + 1

        for i in range(start_date_index, end_index):
            downloads.append(
                {
                    'symbol': symbol,
                    'date': monthly_dates[i],
                    'trade': trade,
                    'period': "monthly",
                    'interval': interval
                }
            )

    if start_date_index == -1 or end_date_index == -1:
        year = int(monthly_date[-1][:4])
        month = int(monthly_date[-1][-2:])
        
        if year == now.year and month == now.month - 1:
            start_index = start.day if start_date_index == -1 else 1
            end_index = now.day if end is None else end.day + 1

            for day in range(start_index, end_index):
                date =f'{now.year}-{now.month:02}-{day:02}'
                downloads.append(
                    {
                        'symbol': symbol,
                        'date': date,
                        'trade': trade,
                        'period': "daily",
                        'interval': interval
                    }
                )

    if truncate:
        offset = 0
        for _i in range(len(downloads)):
            i = _i - offset
            file = __klines_single_file(**downloads[i])
            if file.exists():
                downloads.pop(i)
                offset += 1

    return downloads

async def __download_klines_range(
    symbol: str,
    start: datetime,
    end: datetime,
    trade: str,
    interval: str
) -> List[Path]:
    return await asyncio.gather(
        *[
            __download_klines_single(**kwargs)
            for kwargs in await __get_klines_range_downloads(
                symbol,
                start,
                end,
                trade,
                interval
            )
        ]
    )

async def download_range(
    symbol: str,
    start: datetime,
    end: datetime,
    tdtype: str,
    trade: str,
    interval: str
):
    if tdtype == 'klines':
        return await __download_klines_range(
            symbol,
            start,
            end,
            trade,
            interval
        )
    else:
        raise NotImplementedError(f"{tdtype} retrieval not implemented")
    
async def get_range_downloads(
    symbol: str,
    start: datetime,
    end: datetime,
    tdtype: str,
    trade: str,
    interval: str,
    truncate: bool = False
):
    if tdtype == 'klines':
        downloads = await __get_klines_range_downloads(
            symbol,
            start,
            end,
            trade,
            interval,
            truncate=truncate
        )

        for download in downloads:
            download['tdtype'] = tdtype

        return downloads
    else:
        raise NotImplementedError(f"{tdtype} retrieval not implemented")
    
async def download_single(
    symbol: str,
    date: str,
    tdtype: str,
    trade: str,
    period: str,
    interval: str
):
    if tdtype == 'klines':
        return await __download_klines_single(
            symbol,
            date,
            trade,
            period,
            interval
        )
    else:
        raise NotImplementedError(f"{tdtype} retrieval not implemented")