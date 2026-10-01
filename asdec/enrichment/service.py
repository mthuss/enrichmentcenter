import logging
from sqlalchemy import update, func
from typing import Sequence
from asdec.enrichment.models import ENRICHMENT_INTERVALS, EnrichmentStatus, EnrichmentType, EnrichmentJob, JobInfo
from asdec.enrichment.repo import EnrichmentJobRepo

logger = logging.getLogger(__name__)

# responsible for the meta-management of jobs. 
# Basically, it schedules jobs for the domains that were added 
# to the database.
class EnrichmentService:
    def __init__(self, repo: EnrichmentJobRepo):
        self._repo_ = repo

    async def addJobAfterDomainInsert(self, domain_ids: Sequence[int], enrichment_types: Sequence[EnrichmentType] = [EnrichmentType.LEXICAL, EnrichmentType.RDAP]):
        jobs = []
        for id in domain_ids:
            jobs.extend([{"domain": id, "enrichment_type": t, "status": EnrichmentStatus.PENDING} for t in enrichment_types])
        print(f"jobs: {len(jobs)}")
        await self._repo_.bulk_create(jobs,8192)

    async def get_pending_jobs(self, enrichment_type: EnrichmentType, batch_size: int) -> Sequence[JobInfo]:
        return await self._repo_.claim_batch(enrichment_type, batch_size)

    async def get_jobs_by_status(self, enrichment_status: EnrichmentStatus, enrichment_type: EnrichmentType | None = None, batch_size = 16384):
        return await self._repo_.get_jobs_by_status(enrichment_status, enrichment_type, batch_size)

    async def change_job_status(self, jobs: Sequence[EnrichmentJob], new_status: EnrichmentStatus, new_date=None):
        job_ids = [j.id for j in jobs ]
        return await self._repo_.change_job_status(job_ids, new_status, new_date=new_date)

    async def get_domains_from_jobs(self, jobs: Sequence[EnrichmentJob]):
        domain_ids = [i.domain for i in jobs]


    # update succesful jobs with the COMPLETE EnrichmentStatus and schedule 
    # the next enrichment
    async def mark_done_batch(self, job_ids: Sequence[int], enrichment_type: EnrichmentType):

        enrichment_period = ENRICHMENT_INTERVALS[enrichment_type]

        if not job_ids:
            return

        await self._repo_.mark_done_batch(job_ids, enrichment_period)

#    async def schedule_next_enrichment(self, jobs: Sequence[EnrichmentJob], next_enrichment_dates)
#        if next_enrichment_dates == None:
#            next_enrichment_dates = []
#            for j in jobs:
#                next_enrichment_dates.append({"id": j.id, "next_enrichment": None})
#
#        await self._repo_.schedule_enrichment(job_ids, next_enrichment_dates)

    # the idea here is, if the program was killed
    # while a job was running, the job will be left
    # in a limbo, where it's marked as IN_PROGRESS,
    # so it can't be picked up by an enricher, but
    # it also isn't actually running.
    # As such, this function runs at startup to identify
    # such tasks and reschedule them.
    async def terrible_nogood_version_of_reschedule_broken_jobs_on_startup(self):
        changed = []
        for etype in EnrichmentType:
            while True:
                jobs = await self.get_jobs_by_status(EnrichmentStatus.IN_PROGRESS, etype, 16384)

                if not jobs:
                    break

                results = await self.change_job_status(jobs, EnrichmentStatus.PENDING, new_date=func.now())
                changed.extend(results)

        logger.info(f"{len(changed)} broken jobs rescheduled.")
        return f"{len(changed)} jobs rescheduled."

    async def reschedule_broken_jobs_on_startup(self):
        logger.info("Rescheduling broken jobs.")
        changed = await self._repo_.reschedule_broken_jobs()
        logger.info(f"{changed} broken jobs rescheduled.")