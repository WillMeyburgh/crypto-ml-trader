
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

from crypto_ml_trader import simulator
from crypto_ml_trader.historical.downloader import Downloader
from crypto_ml_trader.simulator import Simulator

async def main():
    simulator = Simulator(
        [
            'ACMTRY',
            'ACMUSDT',
            'ACTBRL',
            'ACTEUR',
        ]
    )

    datasets = await simulator.datasets()

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())