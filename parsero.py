import re
import csv
import json
import httpx
import ipaddress
import tempfile
import requests
from enum import Enum
from typing import Sequence
from fastapi import FastAPI, Request
from urllib.parse import urlparse
from fastapi.responses import FileResponse

class parse_methods(Enum):
    CSV = "csv"                     # parse like CSV file, 1s column with domains, 1 per line
    JSON = "json"                   # parse like JSON file
    HOSTS = "hosts"                 # parse like hosts file
    SIMPLE = "simple"               # parse with comments using feed.parser
    FULL_URL = "full_url"           # parse like full URL, extracting only the domain part
    AD_GUARD = "adguard"            # parse like AdGuard
    AD_BLOCK_PLUS = "adblock_plus"  # parse like Adblock Plus
    SUBDOMAIN = "subdomain"         # parse like simple, removing '.' on the first character of the line

async def parse_feeds(feed_source: str, feed_parser: str, method = None, csv_col = "None") -> Sequence[str]:
        """Fetch feed data from source and store in database"""

        extracted_domains = []  # List of domains names with possible ips
        filtered_domains = []   # List of 100% domains names
        temp_file_path = None

        """header to mimic a real browser request to avoid being blocked by some servers (still being blocked by some servers)"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        """Fetch the feed data from the source URL using requests"""
        async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
            print(f"Fetching data from source: {feed_source} using parser: {feed_parser}")
            
            try:
                response = requests.get(feed_source, timeout=10)
                response.raise_for_status()

                with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
                    temp_file_path = f.name
                    f.write(response.content)

            except requests.exceptions.HTTPError as e:
                print(f"Error HTTP {e.response.status_code} while downloading feed: {feed_source}: {e}")
                return []
            except requests.exceptions.RequestException as e:
                print(f"Network or request error occurred while downloading feed: {feed_source}: {e}")
                return []
            except Exception as e:
                print(f"Unexpected error occurred while processing feed: {feed_source}: {e}")
                return []

        """Parse the downloaded file based on the specified method"""
        print(f"Parsing feed: {feed_source} using method: {method}")
        with open(f"{temp_file_path}", "r", encoding="utf-8", errors="ignore") as f:

            # None method selected, just read the file as is and extract all non-empty lines as domains
            if method is None:
                extracted_domains.extend(
                    line.strip()
                    for line in f
                    if line.strip()
                )

            # CSV method selected, parse the file as CSV and extract the first column as domains
            elif method == "csv":
                sample = f.read(1024)
                if not sample:
                    print("Empty feed file")
                    return []
                
                f.seek(0)

                # Try to extract the dialect being used
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t', '|'])
                    f.seek(0)
                except csv.Error:
                    dialect = csv.excel

                # Handle the csv_col field: has header -> string, no header -> int (position)
                # Also change reader type based on whether the columns have names
                if not csv_col:
                    csv_col = 0
                    reader = csv.reader(f, dialect=dialect)
                elif csv_col.isdigit():
                    csv_col = int(csv_col)
                    reader = csv.reader(f, dialect=dialect)
                else:
                    reader = csv.DictReader(f, dialect=dialect)


                try:
                    for row in reader: # skip empty rows
                        if not row:
                            continue

                        domain_candidate = row[csv_col].strip()

                        if not domain_candidate:
                            continue

                        extracted_domains.append(domain_candidate)
                except:
                    print("Specified column name doesn't exist.")
                    return []
            
            # JSON method selected, parse the file as JSON and extract domains from a list
            elif method == "json":
                try:
                    data = json.load(f)

                    if isinstance(data, list):
                        for domain_candidate in data:
                            domain_candidate = domain_candidate.strip()
                            if domain_candidate:
                                extracted_domains.append(domain_candidate)
                    else:
                        print(f"Unexpected JSON structure in feed: {feed_source}. Expected a list of domains")
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from feed: {feed_source}: {e}")
            
            # HOSTS method selected, parse the file as a hosts file and extract domains from the second column
            elif method == "hosts":
                for line in f:
                    line = line.strip()

                    clean_line = line.split(feed_parser)[0].strip()
                    if not clean_line or clean_line.startswith(feed_parser):
                        continue

                    parts = line.split()

                    if len(parts) >= 2:
                        for domain_candidate in parts[1:]:
                            extracted_domains.append(domain_candidate.strip())

            # SIMPLE method selected, parse the file line by line and extract domains that do not start with feed.parser
            elif method == "simple":
                extracted_domains.extend(
                    line.strip()
                    for line in f
                    if line.strip() and not line.startswith(f"{feed_parser}")
                )

            # FULL_URL method selected, parse the file line by line and extract domains from full URLs
            elif method == "full_url":
                for line in f:
                    line = line.strip()

                    if not line or line.startswith(feed_parser):
                        continue

                    url_to_parse = line
                    if "://" not in line:
                        url_to_parse = f"http://{line}"

                    try:
                        parsed_url = urlparse(url_to_parse)
                        netloc = parsed_url.netloc

                        if netloc:
                            domain_candidate = netloc.split(":")[0]
                            domain_candidate = domain_candidate.strip()

                            if domain_candidate:
                                extracted_domains.append(domain_candidate)

                    except Exception as e:
                        continue
            
            # AD_GUARD method selected, parse the file line by line and extract domains using AdGuard syntax
            elif method == "adguard":
                regex_pattern = r"^\|\|([^\^\$\/\s]+)"

                for line in f:
                    line = line.strip()
                    if not line or line.startswith(("!", "[")) or "*" in line:
                        continue
                    
                    match = re.match(regex_pattern, line)
                    if match:
                        extracted_domains.append(match.group(1))

            # AD_BLOCK_PLUS method selected, parse the file line by line and extract domains using Adblock Plus syntax
            elif method == "adblock_plus":
                regex_pattern = r"^\|\|([a-zA-Z0-9.-]+)(?:[\^/$]|$)|^([a-zA-Z0-9.,-]+)(?:##|#@#|#\?#)"

                for line in f:
                    line = line.strip()
                    if not line or line.startswith(("!", "[", "@@")) or "*" in line:
                        continue
                    
                    match = re.match(regex_pattern, line)
                    if match:
                        domain_str = match.group(1) or match.group(2)
                        if domain_str:
                            for domain in domain_str.split(","):
                                domain = domain.strip()
                                if domain:
                                    extracted_domains.append(domain)

            # parse like simple, removing '.' on the first character of the line
            elif method == "subdomain":
                for line in f:
                    clean_line = line.split(feed_parser)[0].strip()
                    if not clean_line or clean_line.startswith(feed_parser):
                        continue

                    if clean_line.startswith("."):
                        clean_line = clean_line[1:]

                    extracted_domains.append(clean_line)

        print(f"Total domains and possible ips extracted: {len(extracted_domains)}")

        """Filter out any extracted domains that are actually IP addresses, keeping only valid domain names"""
        for domain in extracted_domains:
            try:
                ipaddress.ip_address(domain)
                print(f"Skipping IP address: {domain}")
                continue
            except ValueError:
                filtered_domains.append(domain)

        return filtered_domains

