from asdec.enrichers.base import Enricher
from asdec.core.db import queries
from .service import enrich_domain
import asyncio
import whoisit


class RDAPEnricher(Enricher):
    name = "rdap"
    concurrency = 5  # important for rate limiting

    async def fetch_batch(self):
        return await queries.fetch_domains_due_for_rdap(self.batch_size)

    async def process_item(self, item):
        domain = item["domain"]

        # result = await enrich_domain(domain)
        result = await asyncio.wait_for(
            enrich_domain(domain),
            timeout=5
        )

        item["result"] = result

    async def mark_done(self, item):
        await queries.update_rdap(
            domain_id=item["id"],
            data=item["result"]
        )
    async def handle_error(self, item, error):
        await queries.mark_rdap_failed(item["id"], str(error))