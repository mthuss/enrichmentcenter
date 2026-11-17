import logging
from itertools import batched
from sqlalchemy.dialects.postgresql import insert
from asdec.enrichers.rdap.models import DomainRegistration, RegistrationFeaturesCreate, RegistrationFeatures, RegistrationStatusFlags, RegistrationStatusFlagsCreate
from typing import Sequence
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

class RDAPRepo:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def bulk_create(self, features: Sequence[RegistrationFeaturesCreate], batch_size: int):
        values = [f.model_dump() for f in features]
        statusflags = {}
        statusflagvalues = []
        results = []

        # Map the status flags to the registrable name so they can be added to the DB later
        for v in values:
            statusflags[v["registrable_domain"]] = v.pop("status_flags")

             
        rowcount = 0
        # -----------------------------------------------------------
        # Add the registration features themselves into the database:
        # -----------------------------------------------------------
        for chunk in batched(values, batch_size):
            stmt = (insert(RegistrationFeatures).values(chunk))
            
            # This basically just gets the names of all the columns from the RegistrationFeatures table and then
            update_values = {
                column.name: getattr(stmt.excluded, column.name)
                for column in RegistrationFeatures.__table__.columns
                if column.name != "registrable_domain" and column.name != "id"
            }
            
            # When a conflict happens on the "registrable_domain" field, all values are updated
            # ENHANCEMENT: There's a chance that perfectly good, complete data could be overwritten 
            # with nothing in the case of an error. 
            # Maybe add handling of this later (probably using func.coalesce())
            stmt = stmt.on_conflict_do_update(
                index_elements=["registrable_domain"],
                set_ = update_values
            ).returning( # Return the id and registrable domain to be used for StatusFlags creation
                RegistrationFeatures.registrable_domain,
                RegistrationFeatures.id
            )
            result = await self._session.execute(stmt)
            results.extend(result.all())
            rowcount += len(chunk)


        # -----------------------------------------------------------------
        # Relate the status flags to their corresponding registration data:
        # -----------------------------------------------------------------
        for domain, rid in results:
            flags = statusflags[domain]
            if flags:
                statusflagvalues.extend([{"registration_id": rid, "flag": f} for f in flags])


        # TODO: If a registration status flag is no longer present, it needs to be removed...
        for chunk in batched(statusflagvalues, batch_size):
            stmt = (insert(RegistrationStatusFlags).values(chunk).on_conflict_do_nothing())
            await self._session.execute(stmt)

        await self._session.commit()

        return results, rowcount

    async def associate_domains_to_registration_data(self, values, batch_size):
        for chunk in batched(values, batch_size):
            stmt = (insert(DomainRegistration).values(chunk).on_conflict_do_nothing())
            await self._session.execute(stmt)

        await self._session.commit()
