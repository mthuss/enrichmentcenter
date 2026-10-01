from asdec.enrichers.lexical.worker import LexicalEnricher
from asdec.core.deps import get_domain_service, get_domain_repository, get_list_repository, get_lexical_enricher
from asdec.enrichers.lists.repo import DomainRepo, ListRepo
from asdec.enrichers.lists.service import DomainService
from fastapi import APIRouter, Depends

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