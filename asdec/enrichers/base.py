from enum import Enum
from typing import Sequence, Any
import asyncio
from abc import ABC, abstractmethod

from asdec.enrichment.models import EnrichmentException, EnrichmentJob, EnrichmentResults, JobInfo

class ErrorTypes(Enum):
    TIMED_OUT = 1,
    PROCESSING_ERROR = 2,

class Enricher(ABC):
    name = "base"
    batch_size = 50
    concurrency = 10
    idle_sleep = 1  # seconds when no work found

    def __init__(self):
        self.sem = asyncio.Semaphore(self.concurrency)

    # --- REQUIRED METHODS ---

    @abstractmethod
    async def fetch_batch(self)->Any:
        """Fetch items to process (from DB)"""
        pass

    async def async_process_item(self, item) -> Any:
        """Main enrichment logic"""
        pass

    @abstractmethod 
    def process_item(self, item) -> Any:
        pass

    @abstractmethod
    async def persist_batch(self, successful, errors):
        """Persist successful results in batch"""
        pass
    @abstractmethod
    async def mark_done(self, item):
        """Persist successful result"""
        pass

    # --- OPTIONAL HOOKS ---
    def handle_error(self, item, error):
        """Override for retries / logging"""
        print(f"[{self.name}] error on {item}: {error}")

    # --- INTERNAL EXECUTION LOGIC ---

    async def _run_item(self, item):
        async with self.sem:
            try:
                await self.process_item(item)
                await self.mark_done(item)
            except Exception as e:
                self.handle_error(item, e)


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