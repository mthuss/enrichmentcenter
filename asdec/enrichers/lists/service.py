from typing import Sequence
import requests
import csv
from asdec.enrichers.lists.models import Feed, FeedCreate
from asdec.enrichers.lists.repo import ListRepo, DomainRepo
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from os import path
from asdec.core.config import Settings
from parsero import parse_feeds

# response format:
# {"error": bool, "error_msg" (optional): string, "response": string}

class DomainService:
    def __init__( self, repo: DomainRepo):
        self._repo_ = repo
    def createDomain(self, domain, malicious):
        name = row["name"]

    def getDomainsFromFeed(self, feed: Feed):
        return parse_feeds(feed.url, feed.parse_char, feed.parser, feed.csv_column)

    async def addNewDomains(self, feeds: Sequence[Feed]):
        for feed in feeds:
            domains = await self.getDomainsFromFeed(feed)
            return domains


    

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