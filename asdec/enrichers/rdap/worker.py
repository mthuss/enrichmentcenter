import tldextract
import logging
from typing import Sequence
from asdec.enrichers.base import Enricher
from asdec.enrichment.models import EnrichmentError, EnrichmentException, EnrichmentResults, EnrichmentType, JobInfo
from asdec.enrichment.repo import EnrichmentJobRepo
from asdec.enrichment.service import EnrichmentService
from .service import RDAPService
import asyncio
import whoisit

logger = logging.getLogger(__name__)

class RDAPEnricher(Enricher):
    name = "rdap"
    enrichment_type = EnrichmentType.RDAP
    batch_size = 1000
    concurrency = 10
    idle_sleep = 3

    def __init__(self, db):
        super().__init__()
        # self._rdap_repo = RDAPRepo
        self._db = db
        self._rdap_service = RDAPService(self._db)
        self.semaphore = asyncio.Semaphore(self.concurrency)

        # a list of servers that have caused timeouts so that
        # delays can be adjusted
        self.timed_out_servers = {}

    async def fetch_batch(self):
        job_service = EnrichmentService(EnrichmentJobRepo(self._db.getSession()))
        return await job_service.get_pending_jobs(enrichment_type=EnrichmentType.RDAP, batch_size=self.batch_size)

    async def process_item(self, item: JobInfo):
        async with self.semaphore:
            return await self._rdap_service.enrich_domain(item)

    async def persist_batch(self, successful: Sequence[EnrichmentResults], errors: Sequence[EnrichmentResults]):
        async with self._db.getSession() as session:
            rdap_service = RDAPService(self._db)
            job_service = EnrichmentService(EnrichmentJobRepo(session))
            await rdap_service.add_batch_to_db(successful)
            # await self._job_service.schedule_next_enrichment(successful["items"], None)
            job_ids = [r.job_id for r in successful]
            await job_service.mark_done_batch(job_ids, self.enrichment_type)

    async def _run_batch_item(self, item: JobInfo):
        try:
            results = await self.process_item(item)
            return EnrichmentResults(job_id=item.job.id,domain_id=item.job.domain, results=results, error=None)
        except EnrichmentException as e:
            self.handle_error(item, e)
            return EnrichmentResults(job_id=item.job.id, domain_id=item.job.domain, results=None, error=e.code)

    async def process_batch(self, batch: Sequence[JobInfo]):
        tasks = [self._run_batch_item(item) for item in batch]
        return await asyncio.gather(*tasks)

    async def mark_done(self, item):
        pass

#    async def mark_done(self, item):
#        await queries.update_rdap(
#            domain_id=item["id"],
#            data=item["result"]
#        )
    def handle_error(self, item, error):
        if error.code == EnrichmentError.TIMED_OUT:
            server = self._rdap_service.get_rdap_server(item.domain_name)
            if server in self.timed_out_servers.keys():
                self.timed_out_servers[server] += 1
            else:
                self.timed_out_servers[server] = 0

    async def run_until_empty(self):
        batch_counter = 1
        print(f"[{self.name}] worker started")

        while True:
            batch: Sequence[JobInfo] = await self.fetch_batch()

            if not batch:
                break

            # the idea here is that FQDNs that have subdomains
            # won't have RDAP registration, only the top domain
            # under the public suffix. As such, those are 
            # extracted and the RDAP queries are done for those.
            for job in batch:
                job.domain_name = tldextract.extract(job.domain_name).top_domain_under_public_suffix

            batch = self._rdap_service.interleave_by_rdap_server(batch)
            # logger.info("List of domains that will be processed:")
            # logger.info([d.domain_name for d in batch])

            successful = []
            errors = []

            results = await self.process_batch(batch)

            for res in results:
               if res.error:
                   errors.append(res)
               else:
                   successful.append(res)

            await self.persist_batch(successful, errors)
            batch_counter += 1
            break
