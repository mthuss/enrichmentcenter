# Functions for interfacing with the database
from itertools import batched
from typing import Sequence
import logging
from datetime import datetime
from asdec.enrichers.lists.models import FeedCreate, Feed, DomainCreate, Domain, FeedDomain
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func
logger = logging.getLogger(__name__)
asyncpg_limit = 16_384

class ListRepo:
    def __init__(self, session: AsyncSession):
        self._session_ = session

    async def create(self, feed: FeedCreate):
        stmt =(insert(Feed).values(feed.model_dump()).on_conflict_do_nothing())
        result = await self._session_.execute(stmt)
        await self._session_.commit()
        logger.debug(f"Created feed with name {feed.name}.")
        return result

    async def getAllFeeds(self):
        stmt = select(Feed)
        result = await self._session_.scalars(stmt)
        return result.all()


class DomainRepo:
    def __init__(self, session: AsyncSession):
        self._session_ = session
    
    async def create(self, domain: DomainCreate):
        stmt = (insert(Domain).values(domain.model_dump()).on_conflict_do_nothing())
        result = await self._session_.execute(stmt)
        await self._session_.commit()
        return result
    
    async def bulk_create(self, domains: Sequence[str], feed: Feed):
        # Add domains in bulk to the database
        results = []
        batch_num = 1

        # Add the actual domains
        for chunk in batched(domains, asyncpg_limit):
            batch_num += 1
            stmt = (insert(Domain).values([{"name": name } for name in chunk]).on_conflict_do_nothing().returning(Domain))

            result = await self._session_.execute(stmt)
            results.extend(result.scalars().all())
            await self._session_.commit()

        # Add the FeedDomain intermediate relationship items
        for chunk in batched(results, 8192):
            stmt = (insert(FeedDomain).values([{"domain_id": domain.id, "feed_id": feed.id} for domain in chunk]).on_conflict_do_update(index_elements=["domain_id","feed_id"], set_={"updated_date": func.now()}))
            result = await self._session_.execute(stmt)
            await self._session_.commit()



        return results
