from asdec.enrichers.lists.models import Domain
import logging
from itertools import batched
from typing import Sequence
from datetime import date, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func, update, and_, or_
from asdec.enrichment.models import EnrichmentJob, EnrichmentStatus, EnrichmentType, JobInfo

logger = logging.getLogger(__name__)

class EnrichmentJobRepo:
    def __init__(self, session: AsyncSession):
        self._session_ = session

    async def bulk_create(self, jobs, batch_size: int):
        rowcount = 0
        for chunk in batched(jobs, batch_size):
            stmt = (insert(EnrichmentJob).values(chunk).on_conflict_do_nothing())
            result = await self._session_.execute(stmt)
            rowcount += result.rowcount
        
        await self._session_.commit()
        
        return rowcount

    async def claim_batch(self, enrichment_type: EnrichmentType, batch_size: int):
        # get <batch_size> pending jobs
        subquery = (
            select(EnrichmentJob.id)
            .where(
                EnrichmentJob.enrichment_type == enrichment_type,
                EnrichmentJob.status == EnrichmentStatus.PENDING,
            )
            .order_by(EnrichmentJob.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
            .subquery()
        )

        stmt = (
            update(EnrichmentJob)
            .where(EnrichmentJob.id.in_(select(subquery.c.id)))
            .values(
                status=EnrichmentStatus.IN_PROGRESS,
            )
            .returning(EnrichmentJob)
        )

        result = await self._session_.execute(stmt)
        jobs = result.scalars().all()

        # get domains associated with the jobs:
        domain_ids = [job.domain for job in jobs]

        domains = {}
        for batch in batched(domain_ids, 32000):
            domain_stmt = (
                select(Domain.id, Domain.name)
                .where(Domain.id.in_(batch))
            )

            result = await self._session_.execute(domain_stmt)
            domains.update({
                domain_id: name
                for domain_id, name in result.all()
            })

        await self._session_.commit()

        # returns a list of tuples containing (EnrichmentJob, domain_name (str))
        return [JobInfo(job, domains[job.domain]) for job in jobs]

    async def get_jobs_by_status(self, enrichment_status, enrichment_type, batch_size):
        if enrichment_type:
            stmt = (
                select(EnrichmentJob)
                .where(EnrichmentJob.status == enrichment_status,
                    EnrichmentJob.enrichment_type == enrichment_type)
                .order_by(EnrichmentJob.id)
                .limit(batch_size)
            )
        else:
            stmt = (
                select(EnrichmentJob)
                .where(EnrichmentJob.status == enrichment_status,)
                .order_by(EnrichmentJob.id)
                .limit(batch_size)
            )
        
        result = await self._session_.execute(stmt)
        jobs = result.scalars().all()

        return jobs

    async def change_job_status(self, job_ids: Sequence[int], new_status: EnrichmentStatus, new_date=None):
        results = []
        for batch in batched(job_ids, 8096):
            if new_date is not None:
                stmt = (
                    update(EnrichmentJob)
                    .where(EnrichmentJob.id.in_(batch))
                    .values(
                        status=new_status,
                        next_enrichment=new_date
                    )
                    .returning(EnrichmentJob)
                )
            else:
                stmt = (
                    update(EnrichmentJob)
                    .where(EnrichmentJob.id.in_(batch))
                    .values(
                        status=new_status,
                    )
                    .returning(EnrichmentJob)
                )
        
            res = await self._session_.execute(stmt)
            results.extend(res.scalars().all())

        await self._session_.commit()
        return results

    async def reschedule_broken_jobs(self):
        stmt = (
            update(EnrichmentJob)
            .where(EnrichmentJob.status == EnrichmentStatus.IN_PROGRESS)
            .values(
                status=EnrichmentStatus.PENDING
            )
        )
        res = await self._session_.execute(stmt)
        count = res.rowcount
        await self._session_.commit()
        logger.info(f"{count} domains rescheduled")
        return count


    async def mark_done_batch(self, job_ids, enrichment_interval):
        for batch in batched(job_ids,16384):
            stmt = (
                update(EnrichmentJob)
                .where(EnrichmentJob.id.in_(batch))
                .values(
                    status=EnrichmentStatus.COMPLETE,
                    next_enrichment=(func.now() + timedelta(days=enrichment_interval)) if enrichment_interval else None
                )
            )
            logger.info("[erichment mark_done] Before execution")
            await self._session_.execute(stmt)
            logger.info("[erichment mark_done] After execution, before commit")

        await self._session_.commit()
        logger.info("[erichment mark_done] After commit")

#    async def schedule_enrichment(self, job_ids, next_enrichment_dates):
#        for batch in job_ids:
#            stmt = (update(EnrichmentJob))
#                
            