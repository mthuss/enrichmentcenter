# Functions for interfacing with the database
import logging
from asdec.enrichers.lists.models import FeedCreate, Feed
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
logger = logging.getLogger(__name__)

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
    
    async def create(self, domain):
        stmt = (insert(Feed).values(domain.model_dump()).on_conflict_do_nothing())
        result = await self._session_.execute(stmt)
        await self._session_.commit()
        return result