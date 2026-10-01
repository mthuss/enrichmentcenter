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

    async def add_to_feed(self, domain_ids: Sequence[int], feed: Feed):
        for chunk in batched(domain_ids, 8192):
            stmt = (insert(FeedDomain).values([{"domain_id": domain_id, "feed_id": feed.id} for domain_id in chunk]).on_conflict_do_update(index_elements=["domain_id","feed_id"], set_={"updated_date": func.now()}))
            await self._session_.execute(stmt)
#            await self._session_.commit()
            
    
    async def bulk_create(self, domains: Sequence[str], feed: Feed):
        # Add domains in bulk to the database
        added = []
        batch_num = 1

        # Split the domain list fetched from the feed in chunks
        for chunk in batched(domains, 8192):
            chunk_domains = list(chunk)
            batch_num += 1

            # Add the actual domains to db
            stmt = (insert(Domain).values([{"name": name, "enrichment_status": 0} for name in chunk]).on_conflict_do_nothing().returning(Domain))
            result = await self._session_.execute(stmt)
            added.extend(result.scalars().all()) # features only the domains that were newly added


            # Retrieve the object ids for every domain inserted/modified in this chunk
            stmt = (
                    select(Domain.id)
                    .where(Domain.name.in_(chunk_domains))
                )
            select_res = await self._session_.scalars(stmt)
            chunk_domain_objs = select_res.all()

            # Add the FeedDomain intermediate relationship items
            await self.add_to_feed(chunk_domain_objs, feed)

        await self._session_.commit()

        return added
