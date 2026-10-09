import time
from asdec.core.db import Database
import asyncio
import logging
from typing import Sequence
import requests
import csv
from asdec.enrichers.lists.models import Feed, FeedCreate, DomainCreate
from asdec.enrichers.lists.repo import ListRepo, DomainRepo
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from os import path
from asdec.core.config import Settings
from asdec.enrichment.repo import EnrichmentJobRepo
from asdec.enrichment.service import EnrichmentService
from parsero import parse_feeds

concurrency = 8
# conc = 1: 2369.18s
# conc = 3: 1722.90s
# conc = 8: 1291.64s

# response format:
# {"error": bool, "error_msg" (optional): string, "response": string}

logger = logging.getLogger(__name__)

class DomainService:
    def __init__( self, repo: DomainRepo, enrichmentservice: EnrichmentService, db: Database):
        self._repo_ = repo
        self._enrichmentservice_ = enrichmentservice
        self._db_ = db

    def createDomain(self, row, malicious):
        name = row["name"]
        return DomainCreate(name=name)

    async def getDomainsFromFeed(self, feed: Feed):
        time.sleep(5)
        return await parse_feeds(feed.url, feed.parse_char, feed.parser, feed.csv_column, malicious=feed.malicious)

    async def addDomainsFromFeed(self, feed: Feed):
        async with self._db_.getSession() as session:
            logger.info(f"Adding domains from the {feed.name} feed.")
            domain_repo = DomainRepo(session)
            enrichment_repo = EnrichmentJobRepo(session)
            enrichment_service = EnrichmentService(enrichment_repo)
            domains = await self.getDomainsFromFeed(feed)
            domains = sorted(set(domains))

            if domains:
                results = await domain_repo.bulk_create(domains, feed)
                print(f"Domains added: {len(results)}")
                if len(results) > 0: # this will only work sometimes due to results being dependant on NEW domains, not just successful attempts. Change later
                    logger.info(f"Finished adding domains from the {feed.name} feed.")
                    domain_ids = [d.id for d in results]
                    await enrichment_service.addJobAfterDomainInsert(domain_ids)
                            # maybe add an else here
                        
                    logger.info(f"Feed {feed.name} had {len(results)} new domains added to the database!")



    async def addNewDomains(self, feeds: Sequence[Feed]):
        semaphore = asyncio.Semaphore(concurrency)
        async def process_feed(feed):
            async with semaphore:
                await self.addDomainsFromFeed(feed)
        
        start = time.perf_counter()
        # Go through every feed in the database, 
        # gather all of their domains and add them
        # to the database. If the domain is already 
        # present, update the rel_feed_domain relation's
        # "updated_date" timestamp
        tasks = [process_feed(feed) for feed in feeds]
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start
        logger.info(f"Time spent gathering all feeds: {elapsed}s")
        return "Domains added!"

    

class ListService:
    def __init__(self, repo: ListRepo):
        self._repo_ = repo
    def createFeed(self, row, malicious=False):
        name = row["name"] 
        url = row["source"]
        parse_char = row["parser"]
        parser = row["parse_method"]
        abuse_type = None
        csv_col = None

        if "csv_column" in row.keys():
            csv_col = row["csv_column"]

        if "abuse_type" in row.keys():
            abuse_type = row["abuse_type"]

        return FeedCreate(name=name, url=url, parser=parser, abuse_type=abuse_type, parse_char=parse_char, csv_column=csv_col, malicious=malicious)

    async def populateFeeds(self):
        config = Settings()
        if not path.isfile(config.BLOCKLIST_CSV_PATH):
            print(f"File {config.BLOCKLIST_CSV_PATH} not found!")
            return

        if not path.isfile(config.WHITELIST_CSV_PATH):
            print(f"File {config.WHITELIST_CSV_PATH} not found!")
            return


        # Add whitelists

        f = open(config.WHITELIST_CSV_PATH, "r")
        sample = f.read(1024)
        f.seek(0)
        if not sample:
            print("Whitelists file is empty!")
            return

        # identify dialect
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t', '|'])
        except csv.Error:
            dialect = csv.excel

        try:
            reader = csv.DictReader(f,dialect=dialect)
        except:
            print("Whitelist is missing header")
            return

        for row in reader:
            if not row: 
                continue
            feed = self.createFeed(row,malicious=False)
            await self._repo_.create(feed)
        f.close()


        # Add blocklists

        f = open(config.BLOCKLIST_CSV_PATH, "r")
        sample = f.read(1024)
        f.seek(0)
        if not sample:
            print("Blocklists file is empty!")
            return

        # identify dialect
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t', '|'])
        except csv.Error:
            dialect = csv.excel

        try:
            reader = csv.DictReader(f,dialect=dialect)
        except:
            print("Blocklist is missing header")
            return

        for row in reader:
            if not row: 
                continue
            feed = self.createFeed(row,malicious=True)
            await self._repo_.create(feed)
        f.close()