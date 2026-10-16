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
from datetime import datetime as dt
import re
import whoisit
import asyncwhois
import json
import asyncio
import country_converter as coco
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

    async def enrich_domain(self, job_info: JobInfo):
        info = {}
        try:
            info = await self.registration_info(job_info.domain_name, job_info.extra_data["rdap_supported"])
        except EnrichmentException as e:
            if e.code == EnrichmentError.NOT_FOUND:
                return RegistrationFeaturesCreate(domain_id=job_info.job.domain,available=False,registrar_handle=None,registrar_name=None,registration_date=None,last_updated=None,expiration_date=None,registrar_country=None,dnssec=None,status_flags=None,all_features=False)
            else:
                raise e
        
        if not info:
            return None

        if "status_flags" in info.keys():
            flags = [ RegistrationStatusFlagsCreate(domain=job_info.job.domain, flag=f) for f in info["status_flags"] ]
        else:
            flags = None

        all_features = all(v is not None for v in info.values())

        return RegistrationFeaturesCreate(
            domain_id=job_info.job.domain, 
            available=True,
            registrar_handle=info["registrar_handle"],
            registrar_name=info["registrar_name"],
            registration_date=info["registration_date"],
            last_updated=info["last_updated"],
            expiration_date=info["expiration_date"],
            registrar_country=info["registrar_country"],
            dnssec=info["dnssec"],
            status_flags=flags,
            all_features=all_features
        )

    async def add_batch_to_db(self, results: Sequence[EnrichmentResults]):
        features: Sequence[RegistrationFeaturesCreate] = [r.results for r in results]
        async with self._db.getSession() as session:
            repo = RDAPRepo(session)
            return await repo.bulk_create(features, 3000)

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
                logger.info(f"[{type(e)}] RDAP Exception has occured for {domain}!!!! Exception: {e}")
                exceptions["rdap"] = e
                rdap_info = {}
            logger.info(f"Falling back to whois for {domain}")
        else:
            rdap_info = {}

        # Get whois info as fallback if any fields missing
        try:
            whois_info = await self._from_whois(domain)
        except Exception as e:
            logger.info(f"[{type(e)}] WHOIS Exception has occured for {domain}!!!! Exception: {e}")
            exceptions["whois"] = e
            whois_info = {}

        # TODO: handle <socket.gaierror> exceptions for WHOIS
        # In case the domain had no supported RDAP server and whois did not return anything:
        if not rdap_supported and not whois_info:
            if isinstance(exceptions["whois"], TimeoutError):
                raise EnrichmentException(EnrichmentError.TIMED_OUT)
            if isinstance(exceptions["whois"], NotFoundError):
                raise EnrichmentException(EnrichmentError.NOT_FOUND)
            raise EnrichmentException(EnrichmentError.ERRORED)
            

        # If both RDAP and WHOIS threw exceptions, then we need to pass that to the function that called this one.
        # Here, we assess what happened and convert it to the EnrichmentError type for ease of use.
        if rdap_supported and not rdap_info and not whois_info:
            logger.info(f"No RDAP or WHOIS data for {domain}!!!!")
            # If it was reported as not existing in RDAP and the WHOIS server timed out, there's a good chance
            # the WHOIS server has been deprecated and, as such, is no longer responding. Treat this as a NOT_FOUND.
            if isinstance(exceptions["rdap"], ResourceDoesNotExist) and isinstance(exceptions["whois"], TimeoutError):
                raise EnrichmentException(EnrichmentError.NOT_FOUND)
            if isinstance(exceptions["rdap"], RateLimitedError) and (isinstance(exceptions["whois"], QueryError) or isinstance(exceptions["whois"], TimeoutError)):
                raise EnrichmentException(EnrichmentError.RATE_LIMITED)
            # One of them timed out, meaning the resource _could_ be available and warrants trying again.
            if isinstance(exceptions["rdap"], RateLimitedError) or isinstance(exceptions["whois"], QueryError) or isinstance(exceptions["whois"], TimeoutError):
                raise EnrichmentException(EnrichmentError.TIMED_OUT)
            # If both responses indicate that the resource does not exist, it... well... doesn't exist...
            if isinstance(exceptions["rdap"], ResourceDoesNotExist) and isinstance(exceptions["whois"], NotFoundError):
                raise EnrichmentException(EnrichmentError.NOT_FOUND)
            # Only RDAP raises this error, but it indicates that the TLD is not supported (.jp, for example).
            # Though the same TLD might be supported in WHOIS, if it fell here, then it means the above
            # exceptions didn't catch it and it likely is, indeed, unsupported.
            if isinstance(exceptions["rdap"], UnsupportedError):
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
        return {
            "registrar_handle": registrar_handle,
            "registrar_name": self._get_registrar_name_from_handle(registrar_handle),
            "registration_date": self._normalize_date(rdap_info.get('registration_date')),
            "last_updated": self._normalize_date(rdap_info.get("last_changed_date")),
            "expiration_date": self._normalize_date(rdap_info.get("expiration_date")),
            "registrar_country": self._get_registrar_country(rdap_info, "rdap"),
            "status_flags": self._normalize_status_list(rdap_info.get("status")),
            "dnssec": rdap_info.get("dnssec")
        }

    # try to get registration date from whois as fallback
    async def _from_whois(self, domain):
        _, whois_info = await asyncwhois.aio_whois(domain)
            # print(json.dumps(whois_info, indent=4, default=str))
        registrar_handle = self._get_registrar(whois_info, "whois")
        if isinstance(registrar_handle, str) and len(registrar_handle) >= 16:
            registrar_handle = registrar_handle[:16]
        return {
            "registrar_handle": registrar_handle,
            "registrar_name": self._get_registrar_name_from_handle(registrar_handle),
            "registration_date": self._normalize_date(whois_info.get("creation_date")),
            "last_updated": self._normalize_date(whois_info.get("updated_date")),
            "expiration_date": self._normalize_date(whois_info.get("expiration_date")),
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
                name = registrar[0].get("name")
                if name:
                    reg = self.REGISTRARS_BY_NAME.get(normalize_registrar_name(name))
                    if reg:
                        return reg[0].get("iana_id")
                return registrar[0].get("handle")
        elif type == "whois":
            reg_name = info.get("registrar")
            if reg_name:
                normalized = normalize_registrar_name(reg_name)
                reg = self.REGISTRARS_BY_NAME.get(normalized)
                if reg:
                    return reg[0].get("iana_id")

    # name is pretty self-explanatory
    def _get_registrar_name_from_handle(self, handle):
        if not handle:
            return None
        reg = self.REGISTRARS_BY_ID.get(handle)
        if reg:
            return normalize_registrar_name(reg.get("name"))
        return None

    def _get_registrar_country(self, info, type):
        if type == "rdap":
            # get from entities field
            entities = info.get("entities")
            if entities:
                registrar = entities.get("registrar")
                if not registrar:
                    return None

                address = registrar[0].get("address")
                # if it has an "address" field, try retrieving country directly
                if address:
                    country = address.get("country")
                    if country:
                        return coco.convert(names=[country], to="ISO2", not_found=None)
                # if it doesn't, try retrieving a handle and reference ICANN list
                else:
                    iana_id = registrar[0].get("handle")
                    if iana_id:
                        reg = self.REGISTRARS_BY_ID.get(iana_id)
                        if reg:
                            return reg.get("country")
        elif type == "whois":
            # get directly from the "country" record
            country = info.get("country")
            if country:
                return coco.convert(names=[country], to="ISO2", not_found=None)

            # try retrieving based on the registrar name
            reg_name = info.get("registrar")
            normalized = normalize_registrar_name(reg_name)
            reg = self.REGISTRARS_BY_NAME.get(normalized)
            if reg:
                return reg[0].get("country")


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
