import logging
from itertools import batched
from sqlalchemy.dialects.postgresql import insert
from asdec.enrichers.rdap.models import RegistrationFeaturesCreate, RegistrationFeatures
from typing import Sequence
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

class RDAPRepo:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def bulk_create(self, features: Sequence[RegistrationFeaturesCreate], batch_size: int):
        values = [f.model_dump() for f in features]
        rowcount = 0
        for chunk in batched(values, batch_size):
            stmt = (insert(RegistrationFeatures).values(chunk))
            
            update_values = {
                column.name: getattr(stmt.excluded, column.name)
                for column in RegistrationFeatures.__table__.columns
                if column.name != "domain_id"
            }
            
            stmt.on_conflict_do_update(
                index_elements=[RegistrationFeatures.domain_id],
                set_ = update_values
            )
            result = await self._session.execute(stmt)
            rowcount += result.rowcount

        await self._session.commit()

        return rowcount