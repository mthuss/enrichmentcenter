from typing import Sequence
from asdec.enrichers.lexical.models import LexicalFeaturesCreate
from asdec.enrichers.lexical.repo import LexicalRepo
from scipy.stats import entropy
import numpy as np
import tldextract

from asdec.enrichment.models import EnrichmentResults

class LexicalService:
    def __init__(self, repo: LexicalRepo):
        self._repo_ = repo
    def _shannonEntropy(self, domain: str):
        char_list = list(domain)

        # Calculate frequency of each character
        char_counts = {char: char_list.count(char) for char in set(char_list)}

        # Normalize
        total_chars = len(domain)
        probabilities = np.array([count / total_chars for count in char_counts.values()])

        return float(entropy(probabilities, base=2))
        
    def extractFeatures(self, domain: str, domain_id: int):
        split_domain = domain.split('.')
        ext = extraction = tldextract.extract(domain);

        # Top-level domain
        tld = split_domain[-1]

        # Second-level domain
        sld = split_domain[-2]

        # full suffix (like '.com.br')
        full_suffix = ext.suffix

        # FQDN length
        length = len(domain)

        # number of digits found in the FQDN
        digits = sum(c.isdigit() for c in domain)

        # digit to FQDN length ratio
        digit_ratio = digits/length

        # number of hyphens in the FQDN
        hyphen_count = sum(c == "-" for c in domain)

        # number of "labels" separated by '.' in the FQDN
        label_count = len(split_domain)

        # shannon entropy of the domain (i.e. how random it is; useful for DGAs)
        shannon_entropy = self._shannonEntropy(domain)

        return LexicalFeaturesCreate(
            id=domain_id,
            tld=tld,
            sld=sld,
            suffix=full_suffix,
            length=length,
            digits=digits,
            digit_ratio=digit_ratio,
            hyphen_count=hyphen_count,
            label_count=label_count,
            shannon_entropy=shannon_entropy,
        )

    async def add_batch_to_db(self, results: Sequence[EnrichmentResults]):
        features = [r.results for r in results]
        return await self._repo_.bulk_create(features, 2048)