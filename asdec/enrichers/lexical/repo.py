from itertools import batched
from typing import Sequence
from sqlalchemy.dialects.postgresql import insert
from asdec.enrichers.lexical.models import LexicalFeaturesCreate, LexicalFeatures
from sqlalchemy.ext.asyncio import AsyncSession

class LexicalRepo:
    def __init__(self, session: AsyncSession):
        self._session_ = session

    async def create(self, features: LexicalFeaturesCreate):
        stmt = (insert(LexicalFeatures).values(features.model_dump()).on_conflict_do_nothing())
        result = await self._session_.execute(stmt)
        await self._session_.commit()
        return result

    async def bulk_create(self, features: Sequence[LexicalFeaturesCreate], batch_size: int):
        rowcount = 0
        for chunk in batched(features,batch_size):
            stmt = (insert(LexicalFeatures).values([f.model_dump() for f in chunk]).on_conflict_do_nothing())
            result = await self._session_.execute(stmt)
            rowcount += result.rowcount
        await self._session_.commit()
        return rowcount