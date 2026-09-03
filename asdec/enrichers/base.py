import asyncio
from abc import ABC, abstractmethod


class Enricher(ABC):
    name = "base"
    batch_size = 50
    concurrency = 10
    idle_sleep = 1  # seconds when no work found

    def __init__(self):
        self.sem = asyncio.Semaphore(self.concurrency)

    # --- REQUIRED METHODS ---

    @abstractmethod
    async def fetch_batch(self):
        """Fetch items to process (from DB)"""
        pass

    @abstractmethod
    async def process_item(self, item):
        """Main enrichment logic"""
        pass

    @abstractmethod
    async def mark_done(self, item):
        """Persist successful result"""
        pass

    # --- OPTIONAL HOOKS ---

    async def handle_error(self, item, error):
        """Override for retries / logging"""
        print(f"[{self.name}] error on {item}: {error}")

    # --- INTERNAL EXECUTION LOGIC ---

    async def _run_item(self, item):
        async with self.sem:
            try:
                await self.process_item(item)
                await self.mark_done(item)
            except Exception as e:
                await self.handle_error(item, e)

    async def run_once(self):
        batch = await self.fetch_batch()

        if not batch:
            await asyncio.sleep(self.idle_sleep)
            return

        tasks = [self._run_item(item) for item in batch]
        await asyncio.gather(*tasks)

    async def run_forever(self):
        print(f"[{self.name}] worker started")
        while True:
            await self.run_once()