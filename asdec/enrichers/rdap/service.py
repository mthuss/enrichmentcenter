import logging

from asyncwhois.errors import NotFoundError, QueryError
import tldextract
from itertools import zip_longest
from collections import defaultdict
from typing import Sequence

from whoisit.errors import ParseError, RateLimitedError, RemoteServerError, ResourceAccessDeniedError, ResourceDoesNotExist, UnsupportedError, WhoisItError
from asdec.enrichers.rdap.repo import RDAPRepo
from asdec.core.db import Database
from asdec.enrichers.base import Enricher
from asdec.enrichers.rdap.models import RegistrationFeaturesCreate, RegistrationStatusFlagsCreate
from asdec.enrichment.models import EnrichmentError, EnrichmentException, EnrichmentResults, JobInfo
from .registrars import normalize_registrar_name, _load_registrars
from datetime import datetime as dt, timedelta, UTC
import re
import whoisit
import asyncwhois
import json
import asyncio
import country_converter as coco
import socket
logger = logging.getLogger(__name__)

class RDAPService:
    def __init__(self, db: Database):
        whoisit.bootstrap()
        self._db = db
        self.REGISTRARS_BY_ID, self.REGISTRARS_BY_NAME = _load_registrars()
        self.bootstrap = whoisit.save_bootstrap_data(as_json=False)
        self.tld_to_server = {
            tld: tuple(servers)
            for tlds, servers in self.bootstrap["dns"]["services"]
            for tld in tlds
        }

    def enrich_batch(self):
        pass

    # async def test(self, domain):
        # return await self.registration_info(domain)

    # Gets the registration from self.registration_info info, formats it into a RegistrationFeaturesCreate object and returns the list of associated status flags. 
    # Used by the worker
    async def enrich_domain(self, job_info: JobInfo):
        info = {}
        try:
            info = await self.registration_info(job_info.domain_name, job_info.extra_data["rdap_supported"])
        except EnrichmentException as e:
            if e.code == EnrichmentError.NOT_FOUND or e.code == EnrichmentError.UNSUPPORTED:
                return RegistrationFeaturesCreate(available=False, registrable_domain=job_info.domain_name, registrar_handle=None,registrar_name=None,registration_date=None,last_updated=None,expiration_date=None,age_days=None,registrar_country=None,dnssec=None,status_flags=None,all_features=False)
            else:
                raise e
        
        if not info:
            return None

        # TODO: this needs to be returned separately from the RegistrationFeatures object
        # and be added in a list with the format status_flags[registrable_domain] = list_of_status_flags.
        # This is to change the item creation pipeline to adding the RegistrationFeatures objects into
        # the Database, returning the created id (PK) and the registrable_domain string. After inserting those,
        # the status_flags list created before will now be used to associate the returned pairs of 
        # (regisration_id, registrable_domain) with their respective status flags, creating
        # RegistrationStatusFlagsCreate items with the enrichment_id and then finally adding
        # them to the database.

        all_features = all(v is not None for v in info.values())

        return RegistrationFeaturesCreate(
            available=True,
            registrable_domain = job_info.domain_name,
            registrar_handle=info["registrar_handle"],
            registrar_name=info["registrar_name"],
            registration_date=info["registration_date"],
            last_updated=info["last_updated"],
            expiration_date=info["expiration_date"],
            age_days=info["age_days"],
            registrar_country=info["registrar_country"],
            dnssec=info["dnssec"],
            status_flags=info["status_flags"],
            all_features=all_features
        )

    async def add_batch_to_db(self, results: Sequence[EnrichmentResults]):
        equivalent = {}
        features: Sequence[RegistrationFeaturesCreate] = [r.results for r in results]

        async with self._db.getSession() as session:
            repo = RDAPRepo(session)
            added, rowcount = await repo.bulk_create(features, 3000)

            # Create a mapping between the registered domain name and its correspondent registration_id
            # NOTE: this solution relies on the assumption that all domains that were submitted for insertion
            # will be returned by the bulk_create function. Currently, this is true due to the fact that conflicts
            # always generate updates. Still, pay attention to this whenever making changes to bulk_create.
            added_dict = dict(added)
            domain_registration_rel_objs = []
            for r in results:
                registration_id = added_dict[r.results.registrable_domain]
                domain_registration_rel_objs.append({"registration_id": registration_id, "domain_id": r.domain_id})
                if r.extra_data:
                    eq_jobs = r.extra_data.get("equivalent_jobs")
                    if eq_jobs:
                        domain_registration_rel_objs.extend([{"registration_id": registration_id, "domain_id": j.domain} for j in eq_jobs])

            await repo.associate_domains_to_registration_data(values=domain_registration_rel_objs, batch_size=8196)
            return rowcount

    def get_rdap_server(self, domain):
        tld = domain.rsplit(".", 1)[-1].lower()
        return self.tld_to_server.get(tld, ("unknown",))


    def interleave_by_rdap_server(self, jobs: Sequence[JobInfo]):
        groups = defaultdict(list)

        for job in jobs:
            server = self.get_rdap_server(job.domain_name)
            job.extra_data["rdap_supported"] = False if server == ("unknown",) else True
            groups[server].append(job)

        for key in groups.keys():
            print(f"{key}")
            for job in groups[key]:
                print(f"    {job.domain_name}")

        return [
            job
            for row in zip_longest(*groups.values())
            for job in row
            if job is not None
        ]



    #####################
    ### GATHER DATA ####
    ###################
    async def registration_info(self, domain, rdap_supported = True):
        # if get_rdap_server(domain) == ""
        exceptions = dict()
        # If the domain has a TLD that supports RDAP queries:
        if rdap_supported:
            try:
                # try retrieving RDAP info
                rdap_info = await self._from_rdap(domain)
                # Early exit if RDAP already complete
                if rdap_info and all(v is not None for v in rdap_info.values()):
                    return rdap_info
            except Exception as e:
                # Early exit if RDAP responds and says that the domain does not exist.
                if isinstance(e, ResourceDoesNotExist):
                    raise EnrichmentException(EnrichmentError.NOT_FOUND)
                # logger.info(f"[{type(e)}] RDAP Exception has occurred for {domain}!!!! Exception: {e}")
                exceptions["rdap"] = e
                rdap_info = {}
            logger.info(f"Falling back to whois for {domain}")
        else:
            rdap_info = {}

        # Get whois info as fallback if any fields missing
        try:
            whois_info = await self._from_whois(domain)
        except Exception as e:
            # logger.info(f"[{type(e)}] WHOIS Exception has occurred for {domain}!!!! Exception: {e}")
            exceptions["whois"] = e
            whois_info = {}

        # whois might return a response with no data at all for domains that don't exist.
        # Here, an emtpy "domain_name" field is being treated as an indicator of that.
        if not rdap_info and not whois_info.get("domain_name"):
            raise EnrichmentException(EnrichmentError.NOT_FOUND)

        # REVISE: handle <socket.gaierror> exceptions for WHOIS
        # In case the domain had no supported RDAP server and whois did not return anything:
        if not rdap_supported and not whois_info:
            if isinstance(exceptions["whois"], TimeoutError):
                raise EnrichmentException(EnrichmentError.TIMED_OUT)
            if isinstance(exceptions["whois"], NotFoundError):
                raise EnrichmentException(EnrichmentError.NOT_FOUND)
            # this is an error when the getaddrinfo function gets an invalid hostname
            # I think we can assume this means it's not found?
            if isinstance(exceptions["whois"], socket.gaierror):
                raise EnrichmentException(EnrichmentError.NOT_FOUND)
            raise EnrichmentException(EnrichmentError.ERRORED)
            

        # If both RDAP and WHOIS threw exceptions, then we need to pass that to the function that called this one.
        # Here, we assess what happened and convert it to the EnrichmentError type for ease of use.
        if rdap_supported and not rdap_info and not whois_info:
            logger.info(f"No RDAP or WHOIS data for {domain}!!!!")
            # If RDAP DID respond and it responded that the resource does not exist, then the resource is already considered non-existant, regardless of whois
            # This is already treated above, but is here as a guarantee
            if isinstance(exceptions["rdap"], ResourceDoesNotExist):
                raise EnrichmentException(EnrichmentError.NOT_FOUND)
            if isinstance(exceptions["rdap"], RateLimitedError) and (isinstance(exceptions["whois"], (QueryError,TimeoutError, socket.gaierror))):
                raise EnrichmentException(EnrichmentError.RATE_LIMITED)
            # One of them timed out, meaning the resource _could_ be available and warrants trying again.
            if isinstance(exceptions["rdap"], RateLimitedError) or isinstance(exceptions["whois"], (QueryError, TimeoutError, socket.gaierror)):
                raise EnrichmentException(EnrichmentError.TIMED_OUT)
            # If both responses indicate that the resource does not exist, it... well... doesn't exist...
            if isinstance(exceptions["rdap"], ResourceDoesNotExist) and isinstance(exceptions["whois"], NotFoundError):
                raise EnrichmentException(EnrichmentError.NOT_FOUND)
            # Only RDAP raises this error, but it indicates that the TLD is not supported (.jp, for example).
            # Though the same TLD might be supported in WHOIS, if it fell here, then it means the above
            # exceptions didn't catch it and it likely is, indeed, unsupported.
            if isinstance(exceptions["rdap"], UnsupportedError) and not isinstance(exceptions["whois"], TimeoutError):
                raise EnrichmentException(EnrichmentError.UNSUPPORTED)
            # Generic error for when I don't know what to do with the domain lol
            raise EnrichmentException(EnrichmentError.ERRORED)

        return self._merge_prefer_primary(rdap_info, whois_info)

    # try to get registration data from RDAP
    async def _from_rdap(self, domain):
        rdap_info = await whoisit.domain_async(domain, follow_related=False)
            # rdap_info = rdap_with_control(domain)
            # print(json.dumps(rdap_info, indent=4, default=str))
        registrar_handle = self._get_registrar(rdap_info, "rdap")

        if isinstance(registrar_handle, str) and len(registrar_handle) >= 16:
            registrar_handle = registrar_handle[:16]
        return {
            "registrar_handle": registrar_handle,
            "registrar_name": self._get_registrar_name_from_handle(registrar_handle),
            "registration_date": self._normalize_date(rdap_info.get('registration_date')),
            "last_updated": self._normalize_date(rdap_info.get("last_changed_date")),
            "expiration_date": self._normalize_date(rdap_info.get("expiration_date")),
            "age_days": self._get_age(rdap_info.get("registration_date")),
            "registrar_country": self._get_registrar_country(rdap_info, "rdap"),
            "status_flags": self._normalize_status_list(rdap_info.get("status")),
            "dnssec": rdap_info.get("dnssec")
        }

    # try to get registration date from whois as fallback
    async def _from_whois(self, domain):
        _, whois_info = await asyncwhois.aio_whois(domain, timeout=20)
            # print(json.dumps(whois_info, indent=4, default=str))
        registrar_handle = self._get_registrar(whois_info, "whois")
        if isinstance(registrar_handle, str) and len(registrar_handle) >= 16:
            registrar_handle = registrar_handle[:16]
        return {
            "registrar_handle": registrar_handle,
            "registrar_name": self._get_registrar_name_from_handle(registrar_handle),
            "registration_date": self._normalize_date(whois_info.get("created")),
            "last_updated": self._normalize_date(whois_info.get("updated")),
            "expiration_date": self._normalize_date(whois_info.get("expires")),
            "age_days": self._get_age(whois_info.get("created")),
            "registrar_country": self._get_registrar_country(whois_info, "whois"),
            "status_flags": self._normalize_status_list(whois_info.get("status")),
            "dnssec": "true" if whois_info.get("dnssec") == "signed" else "false",
        }

    #####################
    ###### HELPERS #####
    ###################
    # merge data obtained from both RDAP and WHOIS, with RDAP being given the priority
    def _merge_prefer_primary(self, primary, fallback):
        result = primary.copy()

        for key, value in fallback.items():
            if result.get(key) is None:
                result[key] = value

        return result

    # get registrar IANA handle from RDAP or WHOIS record
    def _get_registrar(self, info, type):
        if type == "rdap":
            entities = info.get("entities")
            if entities:
                registrar = entities.get("registrar")
                if not registrar:
                    return None
                handle = registrar[0].get("handle")
                if handle:
                    return handle if handle != "NOT APPLICABLE" else None
                name = registrar[0].get("name")
                if name:
                    reg = self.REGISTRARS_BY_NAME.get(normalize_registrar_name(name))
                    if reg:
                        handle = reg[0].get("iana_id")
                        return handle if handle != "NOT APPLICABLE" else None
                return None
        elif type == "whois":
            reg_name = info.get("registrar")
            if reg_name:
                normalized = normalize_registrar_name(reg_name)
                reg = self.REGISTRARS_BY_NAME.get(normalized)
                if reg:
                    handle = reg[0].get("registrar_iana_id")
                    return handle if handle != "NOT APPLICABLE" else None

    # name is pretty self-explanatory
    def _get_registrar_name_from_handle(self, handle):
        if not handle:
            return None
        reg = self.REGISTRARS_BY_ID.get(handle)
        if reg:
            # maybe the names shouldn't really be normalized here.
            # In theory, since they all come from the same source
            # (i.e., the registrars.csv file), they should always
            # look the same, and they're never used in any other
            # operation that would require them to be normalized.
            #return normalize_registrar_name(reg.get("name"))

            # Returning the non-normalized name gives better readability.
            return reg.get("name")
        return None

    def _get_registrar_country(self, info, type):
        if type == "rdap":
            # get from entities field
            entities = info.get("entities")
            if entities:
                registrar = entities.get("registrar")
                if not registrar:
                    return None

                # try retrieving a handle and reference ICANN list first, as this is technically more correct if available
                iana_id = registrar[0].get("handle")
                if iana_id:
                    reg = self.REGISTRARS_BY_ID.get(iana_id)
                    if reg:
                        countryname = reg.get("country")
                        if isinstance(countryname, str):
                            return countryname
                        else:
                            return None

                address = registrar[0].get("address")
                # if it has an "address" field, try retrieving country directly
                if address:
                    country = address.get("country")
                    if country:
                        return coco.convert(names=[country], to="ISO2", not_found=None)
            return None
        elif type == "whois":
            # try retrieving the country from the registrar IANA ID first
            iana_id = info.get("registrar_iana_id")
            if iana_id:
                reg = self.REGISTRARS_BY_ID.get(iana_id)
                if reg:
                    countryname = reg[0].get("country")
                    if countryname:
                        if isinstance(countryname, str):
                            return countryname
                        else:
                            return None
            # get directly from the "country" record
            country = info.get("country")
            if country:
                return coco.convert(names=[country], to="ISO2", not_found=None)

            # try retrieving based on the registrar name
            reg_name = info.get("registrar")
            normalized = normalize_registrar_name(reg_name)
            reg = self.REGISTRARS_BY_NAME.get(normalized)
            if reg:
                countryname = reg[0].get("country")
                if isinstance(countryname, str):
                    return countryname
                else:
                    return None
            return None


    ########################
    ##### NORMALIZERS #####
    ######################
    def _normalize_status_list(self, status_list):
        if not status_list:
            return []

        # in case the status list is just a single item and not a list. Not pretty, but probably the best solution given the rest of the code.
        if not isinstance(status_list, list):
            status_list = [status_list]

        normalized = set()
        for raw in status_list:
            # remove urls
            token = re.sub(r'https?://\S+', '', raw)
            # remove parenthesis
            token = re.sub(r'\(.*?\)', '', token)
            token = token.strip()

            # replace spaces with _
            if " " in token:
                token = token.lower().replace(" ","_")

            # camelCase to snake_case
            token = re.sub(r'([a-z])([A-Z])', r'\1_\2', token)
            token = token.lower()

            if token:
                normalized.add(token)

        return sorted(normalized)

    def _normalize_date(self, date):
        if date:
            if isinstance(date, list):
                date = date[0]
            return date.strftime("%Y-%m-%d")
        return None

    def _get_age(self, registration_date):
        if registration_date:
            try:
                # get the difference between now and the registration date,
                # offset to registration_date's timezone
                diff = dt.now(registration_date.tzinfo) - registration_date
                age = diff.days
                return int(age)
            except:
                return None
        return None
