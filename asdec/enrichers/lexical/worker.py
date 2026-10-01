import logging
from typing import Sequence, Any
import asyncio
from asdec.enrichers.base import Enricher
from asdec.enrichers.lexical.service import LexicalService
from asdec.enrichment.models import EnrichmentJob, EnrichmentResults, EnrichmentType, JobInfo
from asdec.enrichment.service import EnrichmentService

logger = logging.getLogger(__name__)

class LexicalEnricher(Enricher):
    name = "lexical"
    batch_size = 16000
    concurrency = 10

    def __init__(self, _lexical_service: LexicalService, _job_service: EnrichmentService):
        super().__init__()
        self._lexical_service = _lexical_service
        self._job_service = _job_service

    async def fetch_batch(self) -> Sequence[JobInfo]:
        return await self._job_service.get_pending_jobs(enrichment_type=EnrichmentType.LEXICAL, batch_size=self.batch_size)

    async def mark_done(self, item):
        pass

    async def persist_batch(self, successful: Sequence[EnrichmentResults], errors: Sequence[EnrichmentResults]):
        await self._lexical_service.add_batch_to_db(successful)
        # await self._job_service.schedule_next_enrichment(successful["items"], None)
        job_ids = [r.job_id for r in successful]
        await self._job_service.mark_done_batch(job_ids, EnrichmentType.LEXICAL)

    async def process_item(self, item: JobInfo):
        job = item.job
        domain_name = item.domain_name
        return self._lexical_service.extractFeatures(domain_name, job.domain)

    async def run_until_empty(self):
        batch_counter = 1
        print(f"[{self.name}] worker started")

        while True:
            batch: Sequence[JobInfo] = await self.fetch_batch()

            if not batch:
                break

            logger.info(f"[Batch {batch_counter}] Fetched a batch of {len(batch)} domains.")

            successful = []
            errors = []


            tasks = [self._run_batch_item(item) for item in batch]
            results = await asyncio.gather(*tasks)
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