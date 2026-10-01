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
        added = []
        for chunk in batched(jobs, batch_size):
            stmt = (insert(EnrichmentJob).values(chunk).on_conflict_do_nothing().returning(EnrichmentJob))
            result = await self._session_.execute(stmt)
            added.extend(result.scalars().all())
        
        await self._session_.commit()
        
        return added

    async def claim_batch(self, enrichment_type: EnrichmentType, batch_size: int):
        # get <batch_size> pending jobs
        subquery = (
            select(EnrichmentJob.id)
            .where(
                and_(
                    EnrichmentJob.enrichment_type == enrichment_type,
                    or_(
                    EnrichmentJob.status == EnrichmentStatus.PENDING,
                        and_(
                            EnrichmentJob.next_enrichment != None,
                            EnrichmentJob.next_enrichment <= func.now()
                        )
                    )
                )
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
        domain_stmt = (
            select(Domain.id, Domain.name)
            .where(Domain.id.in_(domain_ids))
        )

        result = await self._session_.execute(domain_stmt)
        domains = {
            domain_id: name
            for domain_id, name in result.all()
        }

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
        stmt = (
            update(EnrichmentJob)
            .where(EnrichmentJob.id.in_(job_ids))
            .values(
                status=EnrichmentStatus.COMPLETE,
                next_enrichment=(func.now() + timedelta(days=enrichment_interval)) if enrichment_interval else None
            )
        )
        await self._session_.execute(stmt)

        await self._session_.commit()

#    async def schedule_enrichment(self, job_ids, next_enrichment_dates):
#        for batch in job_ids:
#            stmt = (update(EnrichmentJob))
#                
            