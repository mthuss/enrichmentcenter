import tldextract
import logging
from typing import Sequence
from asdec.enrichers.base import Enricher
from asdec.enrichment.models import ENRICHMENT_ERROR_TO_STATUS, EnrichmentError, EnrichmentException, EnrichmentResults, EnrichmentType, JobInfo
from asdec.enrichment.repo import EnrichmentJobRepo
from asdec.enrichment.service import EnrichmentService
from .service import RDAPService
import asyncio
from datetime import date, timedelta
import whoisit
import time

logger = logging.getLogger(__name__)

class RDAPEnricher(Enricher):
    name = "rdap"
    enrichment_type = EnrichmentType.RDAP
    batch_size = 1000
    concurrency = 50
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
        # TODO: perhaps replace the "equivalent_jobs" approach by just selecting and marking every single
        # domain that is a subdomain of the queried domain at the mark_done() step.
        async with self._db.getSession() as session:
            rdap_service = RDAPService(self._db)
            job_service = EnrichmentService(EnrichmentJobRepo(session))

            await rdap_service.add_batch_to_db(successful)
            # await job_service.schedule_next_enrichment(successful, date.today() + timedelta(days=14))

            successful_ids = []
            for r in successful:
                successful_ids.append(r.job_id)
                if r.extra_data:
                    if "equivalent_jobs" in r.extra_data.keys():
                        successful_ids.extend([i for i in r.extra_data["equivalent_jobs"]])

            await job_service.mark_done_batch(successful_ids, self.enrichment_type)

            errored_ids = {}
            for e in errors:
                if e.error not in errored_ids.keys():
                    errored_ids[e.error] = [e.job_id]
                else:
                    errored_ids[e.error].append(e.job_id)
                if e.extra_data:
                    if "equivalent_jobs" in e.extra_data.keys():
                        errored_ids[e.error].extend([i for i in e.extra_data["equivalent_jobs"]])

            for key in errored_ids.keys():
                errored_of_type = errored_ids[key]
                await job_service.change_job_status(errored_of_type, ENRICHMENT_ERROR_TO_STATUS[key], self.enrichment_type)
                

    async def _run_batch_item(self, item: JobInfo):
        try:
            results = await self.process_item(item)
            return EnrichmentResults(job_id=item.job.id,domain_id=item.job.domain, results=results, error=None, extra_data=item.extra_data)
        except EnrichmentException as e:
            self.handle_error(item, e)
            return EnrichmentResults(job_id=item.job.id, domain_id=item.job.domain, results=None, error=e.code, extra_data=item.extra_data)

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
        total_domains = 0
        duplicate_jobs = 0
        effective_domains = 0
        print(f"[{self.name}] worker started")

        start = time.perf_counter()
        while True:
            batch: Sequence[JobInfo] = await self.fetch_batch()

            if not batch:
                break

            total_domains += len(batch)
            # the idea here is that FQDNs that have subdomains
            # won't have RDAP registration, only the top domain
            # under the public suffix. As such, those are 
            # extracted and the RDAP queries are done for those.
            seen_domains = {}
            jobs = []
            for item in batch:
                domain_name = tldextract.extract(item.domain_name).top_domain_under_public_suffix
                # Make sure the simplified domains are unique
                if domain_name in seen_domains.keys():
                    seen_domains[domain_name].extra_data["equivalent_jobs"].append(item.job.id)
                    duplicate_jobs += 1
                else:
                    item.domain_name = domain_name
                    seen_domains[domain_name] = item
                    seen_domains[domain_name].extra_data = {"equivalent_jobs": []}
                    jobs.append(item)

            jobs = self._rdap_service.interleave_by_rdap_server(jobs)
            # logger.info("List of domains that will be processed:")
            # logger.info([d.domain_name for d in batch])

            effective_domains += len(jobs)
            successful = []
            errors = []

            results = await self.process_batch(jobs)

            for res in results:
               if res.error:
                   errors.append(res)
               else:
                   successful.append(res)

            await self.persist_batch(successful, errors)
            if batch_counter > 1:
                break
            batch_counter += 1
        elapsed = time.perf_counter() - start
        logger.info(f"Enriched {total_domains} domains in {elapsed:.2f} seconds.")
        logger.info(f"Effective throughput: {(effective_domains/elapsed):.2f} jobs/second.")
        logger.info(f"{duplicate_jobs} duplicate jobs.")
