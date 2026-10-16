import logging
from itertools import batched
from sqlalchemy.dialects.postgresql import insert
from asdec.enrichers.rdap.models import RegistrationFeaturesCreate, RegistrationFeatures, RegistrationStatusFlags
from typing import Sequence
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

class RDAPRepo:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def bulk_create(self, features: Sequence[RegistrationFeaturesCreate], batch_size: int):
        # TODO: need to also mark the domains and jobs contained in
        # job.extra_data.other_ids as complete
        values = [f.model_dump() for f in features]
        statusflags = []
        for v in values:
            flags = v.pop("status_flags")
            if flags:
                # flags is already a list of RegistrationStatusFlagsCreate object
                # and, as such, its items already contain domain_id info
                statusflags.extend([f for f in flags]) 
                # for f in flags:
                    # statusflags.append({"domain": v["domain_id"], "flag": f})

             
        rowcount = 0
        for chunk in batched(values, batch_size):
            stmt = (insert(RegistrationFeatures).values(chunk))
            
            update_values = {
                column.name: getattr(stmt.excluded, column.name)
                for column in RegistrationFeatures.__table__.columns
                if column.name != "domain_id"
            }
            
            stmt = stmt.on_conflict_do_update(
                index_elements=[RegistrationFeatures.domain_id],
                set_ = update_values
            )
            result = await self._session.execute(stmt)
            rowcount += result.rowcount

        for chunk in batched(statusflags, batch_size):
            stmt = (insert(RegistrationStatusFlags).values(chunk).on_conflict_do_nothing())
            await self._session.execute(stmt)

        await self._session.commit()

        return rowcount
