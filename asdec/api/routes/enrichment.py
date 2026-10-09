from asdec.enrichers.rdap.worker import RDAPEnricher
from asdec.enrichers.lexical.worker import LexicalEnricher
from asdec.core.deps import get_domain_service, get_domain_repository, get_list_repository, get_lexical_enricher, get_rdap_service, get_rdap_enricher
from asdec.enrichers.lists.repo import DomainRepo, ListRepo
from asdec.enrichers.lists.service import DomainService
from fastapi import APIRouter, Depends
from pydantic import BaseModel

router = APIRouter(prefix="/enrichment")

@router.post("/start")
async def start_enrichment(
    repo: ListRepo = Depends(get_list_repository),
    service: DomainService = Depends(get_domain_service)
):
    feeds = await repo.getAllFeeds()
    return await service.addNewDomains(feeds)
    
@router.get("/lexical_enrich")
async def lexical_enrich(worker: LexicalEnricher = Depends(get_lexical_enricher)):
    return await worker.run_until_empty()

@router.get("/lexical_enrich_chunk")
async def lexical_enrich_chunk(worker: LexicalEnricher = Depends(get_lexical_enricher)):
    return await worker.run_until_empty_chunks()

@router.get("/lexical_perf_test")
async def lexical_perf_test(worker: LexicalEnricher = Depends(get_lexical_enricher)):
    return await worker.perf_test()


@router.get("/rdap_enrich")
async def rdap_enrich(worker: RDAPEnricher = Depends(get_rdap_enricher)):
    return await worker.run_until_empty()

class Query(BaseModel):
    name: str
@router.post("/rdap_test")
async def rdap_test(query: Query, service = Depends(get_rdap_service)):
    print("Querying " + query.name)
    res = await service.test(query.name)
    print(res)
    return "Yeah"