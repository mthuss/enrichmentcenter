from asdec.core.deps import get_domain_service, get_domain_repository, get_list_repository
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
    