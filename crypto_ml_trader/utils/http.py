from pathlib import Path

import aiofiles
import aiohttp


async def download_url(url: str, destination: Path):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            async with aiofiles.open(destination, mode='wb') as f:
                data = await resp.read()
                await f.write(data)