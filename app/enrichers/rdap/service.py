from registrars import normalize_registrar_name, REGISTRARS_BY_ID, REGISTRARS_BY_NAME
from datetime import datetime as dt
import re
import whoisit
import whois
import json

#####################
### GATHER DATA ####
###################
def registrationInfo(domain):
    # rdap = getRDAP(domain)

    # try retrieving RDAP info
    rdap_info = _from_rdap(domain)

    # Early exit if RDAP already complete
    if rdap_info and all(v is not None for v in rdap_info.values()):
        return rdap_info

    # Get whois info as fallback if any fields missing
    whois_info = _from_whois(domain)

    return _merge_prefer_primary(rdap_info, whois_info)

# try to get registration data from RDAP
def _from_rdap(domain):
    try:
        rdap_info = whoisit.domain(domain)
        # rdap_info = rdap_with_control(domain)
        print(json.dumps(rdap_info, indent=4, default=str))
    except Exception as e:
        print(f"RDAP Error for {domain}: {e}")
        return {}
    registrar_handle = _get_registrar(rdap_info, "rdap")
    return {
        "registrar_handle": registrar_handle,
        "registrar_name": _get_registrar_name_from_handle(registrar_handle),
        "registration_date": _normalize_date(rdap_info.get('registration_date')),
        "last_updated": _normalize_date(rdap_info.get("last_changed_date")),
        "expiration_date": _normalize_date(rdap_info.get("expiration_date")),
        "registrar_country": _get_registrar_country(rdap_info, "rdap"),
        "status_flags": _normalize_status_list(rdap_info.get("status")),
        "dnssec": rdap_info.get("dnssec")
    }

# try to get registration date from whois as fallback
def _from_whois(domain):
    try:
        whois_info = whois.whois(domain)
        print(json.dumps(whois_info, indent=4, default=str))
    except Exception as e:
        print(f"WHOIS Error for {domain}: {e}")
        return {}
    registrar_handle = _get_registrar(whois_info, "whois")
    return {
        "registrar_handle": registrar_handle,
        "registrar_name": _get_registrar_name_from_handle(registrar_handle),
        "registration_date": _normalize_date(whois_info.get("creation_date")),
        "last_updated": _normalize_date(whois_info.get("updated_date")),
        "expiration_date": _normalize_date(whois_info.get("expiration_date")),
        "registrar_country": _get_registrar_country(whois_info, "whois"),
        "status_flags": _normalize_status_list(whois_info.get("status")),
        "dnssec": "true" if whois_info.get("dnssec") == "signed" else "false",
    }

#####################
###### HELPERS #####
###################
# merge data obtained from both RDAP and WHOIS, with RDAP being given the priority
def _merge_prefer_primary(primary, fallback):
    result = primary.copy()

    for key, value in fallback.items():
        if result.get(key) is None:
            result[key] = value

    return result

# get registrar IANA handle from RDAP or WHOIS record
def _get_registrar(info, type):
    if type == "rdap":
        entities = info.get("entities")
        if entities:
            registrar = entities.get("registrar")
            if not registrar:
                return None
            name = registrar[0].get("name")
            if name:
                reg = REGISTRARS_BY_NAME.get(normalize_registrar_name(name))
                if reg:
                    return reg[0].get("iana_id")
            return registrar[0].get("handle")
    elif type == "whois":
        reg_name = info.get("registrar")
        if reg_name:
            normalized = normalize_registrar_name(reg_name)
            reg = REGISTRARS_BY_NAME.get(normalized)
            if reg:
                return reg[0].get("iana_id")

# name is pretty self-explanatory
def _get_registrar_name_from_handle(handle):
    if not handle:
        return None
    reg = REGISTRARS_BY_ID.get(handle)
    if reg:
        return normalize_registrar_name(reg.get("name"))
    return None

def _get_registrar_country(info, type):
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
                    return country
            # if it doesn't, try retrieving a handle and reference ICANN list
            else:
                iana_id = registrar[0].get("handle")
                if iana_id:
                    reg = REGISTRARS_BY_ID.get(iana_id)
                    if reg:
                        return reg.get("country")
    elif type == "whois":
        # get directly from the "country" record
        country = info.get("country")
        if country:
            return country

        # try retrieving based on the registrar name
        reg_name = info.get("registrar")
        normalized = normalize_registrar_name(reg_name)
        reg = REGISTRARS_BY_NAME.get(normalized)
        if reg:
            return reg[0].get("country")


########################
##### NORMALIZERS #####
######################
def _normalize_status_list(status_list):
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

def _normalize_date(date):
    if date:
        return date.strftime("%Y%m%d-%H:%M:%S")
    return None