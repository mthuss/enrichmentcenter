from concurrent.futures import ProcessPoolExecutor
from asdec.enrichers.lexical.repo import LexicalRepo
from asdec.core.db import Database
import logging
from typing import Sequence, Any
import asyncio
from asdec.enrichers.base import Enricher
from asdec.enrichers.lexical.service import LexicalService, extract_lexical_features, pure_extract_lexical_features
from asdec.enrichment.models import EnrichmentException, EnrichmentJob, EnrichmentResults, EnrichmentType, JobInfo
from asdec.enrichment.repo import EnrichmentJobRepo
from asdec.enrichment.service import EnrichmentService
from itertools import chain
import time

logger = logging.getLogger(__name__)

class LexicalEnricher(Enricher):
    name = "lexical"
    enrichment_type = EnrichmentType.LEXICAL
    batch_size = 100000
    concurrency = 10

    def __init__(self, _lexical_service: LexicalService, _job_service: EnrichmentService, db: Database):
        super().__init__()
        self._lexical_service = _lexical_service
        self._job_service = _job_service
        self._db_ = db
        self._executor = ProcessPoolExecutor(max_workers=10)

    async def fetch_batch(self) -> Sequence[JobInfo]:
        return await self._job_service.get_pending_jobs(enrichment_type=self.enrichment_type, batch_size=self.batch_size)

    def _run_batch_item(self, item: JobInfo):
        try:
            results = self.process_item(item)
            return EnrichmentResults(job_id=item.job.id,domain_id=item.job.domain, results=results, error=None)
        except EnrichmentException as e:
            self.handle_error(item, e)
            return EnrichmentResults(job_id=item.job.id, domain_id=item.job.domain, results=None, error=e.code)

    async def mark_done(self, item):
        pass

    async def persist_batch(self, successful: Sequence[EnrichmentResults], errors: Sequence[EnrichmentResults]):
        async with self._db_.getSession() as session:
            lexical_service = LexicalService(LexicalRepo(session))
            job_service = EnrichmentService(EnrichmentJobRepo(session))
            await lexical_service.add_batch_to_db(successful)
            # await self._job_service.schedule_next_enrichment(successful["items"], None)
            job_ids = [r.job_id for r in successful]
            await job_service.mark_done_batch(job_ids, self.enrichment_type)

    def process_item(self, item: JobInfo):
        return extract_lexical_features(item.domain_name, item.job.domain)
    
    async def custom_run_batch_item(self, items: Sequence[JobInfo]):
        results = []
        async with self.sem:
            for item in items:
                try:
                    res = self.process_item(item)
                    results.append(EnrichmentResults(job_id=item.job.id,domain_id=item.job.domain, results=res, error=None))
                except EnrichmentException as e:
                    self.handle_error(item, e)
                    results.append(EnrichmentResults(job_id=item.job.id, domain_id=item.job.domain, results=None, error=e.code))
        return results

    async def run_until_empty_chunks(self):
        batch_counter = 1
        print(f"[{self.name}] worker started")
        loop = asyncio.get_running_loop()

        while True:
            batch: Sequence[JobInfo] = await self.fetch_batch()

            if not batch:
                break

            chunk_size = 10000
            chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]

            successful = []
            errors = []
            results = []

            logger.info(f"[Batch {batch_counter}] Fetched a batch of {len(batch)} domains.")
            tasks = [ loop.run_in_executor(self._executor, self.custom_run_batch_item, chunk) for chunk in chunks ]
            results = await asyncio.gather(*tasks)
            results = list(chain.from_iterable(results))
            logger.info(f"[Batch {batch_counter}] Finished processing batch.")
            for res in results:
                if res.error:
                    errors.append(res)
                else:
                    successful.append(res)

            await self.persist_batch(successful, errors)
            logger.info(f"[Batch {batch_counter}] {len(successful)}/{len(batch)} domains enriched succesfully. {len(errors)} errors.")
            batch_counter += 1

        print(f"[{self.name}] no pending jobs")
        return "All pending jobs completed"

    async def run_until_empty(self):
        batch_counter = 1
        print(f"[{self.name}] worker started")

        while True:
            batch: Sequence[JobInfo] = await self.fetch_batch()

            if not batch:
                break


            successful = []
            errors = []


            logger.info(f"[Batch {batch_counter}] Fetched a batch of {len(batch)} domains.")
            results = [self._run_batch_item(item) for item in batch]
            logger.info(f"[Batch {batch_counter}] Finished processing batch.")
            #results = await asyncio.gather(*tasks)
            for res in results:
                if res.error:
                    errors.append(res)
                else:
                    successful.append(res)

            await self.persist_batch(successful, errors)
            logger.info(f"[Batch {batch_counter}] {len(successful)}/{len(batch)} domains enriched succesfully. {len(errors)} errors.")
            batch_counter += 1

        print(f"[{self.name}] no pending jobs")
        return "All pending jobs completed"

    async def perf_test(self):
        logger.info("Starting lexical features performance test.")
        logger.info(f"Batch size: {self.batch_size}")
        batch: Sequence[JobInfo] = await self.fetch_batch()
        domains = [d.domain_name for d in batch]
        start = time.perf_counter()
        for domain in domains:
            pure_extract_lexical_features(domain)
        elapsed = time.perf_counter() - start
        logger.info(f"{len(domains):,} domains in {elapsed:.3f}s")
        logger.info(f"{len(domains) / elapsed:,.0f} domains/s")