import asyncio
from enrichmentcenter.enrichers.rdap.service import enrich_domain

async def main():
    domain = "rainyseasons.net"
    result = await enrich_domain(domain)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
