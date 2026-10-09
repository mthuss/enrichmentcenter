import logging
from itertools import batched
from typing import Sequence
from sqlalchemy.dialects.postgresql import insert
from asdec.enrichers.lexical.models import LexicalFeaturesCreate, LexicalFeatures
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

class LexicalRepo:
    def __init__(self, session: AsyncSession):
        self._session_ = session

    async def create(self, features: LexicalFeaturesCreate):
        stmt = (insert(LexicalFeatures).values(features.model_dump()).on_conflict_do_nothing())
        result = await self._session_.execute(stmt)
        await self._session_.commit()
        return result

    async def bulk_create(self, features: Sequence[LexicalFeaturesCreate], batch_size: int):
        values = [f.model_dump() for f in features]
        rowcount = 0
        for chunk in batched(values,batch_size):
            stmt = (insert(LexicalFeatures).values(chunk).on_conflict_do_nothing())
            logger.info("[lexical bulk_create] Before execution")
            result = await self._session_.execute(stmt)
            logger.info("[lexical bulk_create] After execution, before commit")
            rowcount += result.rowcount

        await self._session_.commit()
        logger.info("[lexical bulk_create] After commit")

        return rowcount