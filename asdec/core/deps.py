from asdec.enrichers.rdap.worker import RDAPEnricher
from asdec.enrichers.rdap.service import RDAPService
from asdec.enrichers.lexical.service import LexicalService
from asdec.enrichers.lexical.repo import LexicalRepo
from asdec.enrichers.lexical.worker import LexicalEnricher
from asdec.enrichers.lists.service import DomainService, ListService
from asdec.enrichers.lists.repo import DomainRepo, ListRepo
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends, Request
from asdec.core.db import Database
from asdec.core.config import Settings
from asdec.enrichment.repo import EnrichmentJobRepo
from asdec.enrichment.service import EnrichmentService

def get_db(request: Request) -> Database:
    return request.app.state.db

def getSettings():
    return Settings()

async def get_async_session(db: Database = Depends(get_db)):
    async with db.getSession() as session:
        yield session

def get_domain_repository(session: AsyncSession = Depends(get_async_session)) -> DomainRepo:
    return DomainRepo(session)

def get_enrichment_job_repository(session: AsyncSession = Depends(get_async_session)) -> EnrichmentJobRepo:
    return EnrichmentJobRepo(session)

def get_lexical_repository(session: AsyncSession = Depends(get_async_session)):
    return LexicalRepo(session)

def get_enrichment_service(enrichmentjob_repo: EnrichmentJobRepo = Depends(get_enrichment_job_repository)) -> EnrichmentService:
    return EnrichmentService(enrichmentjob_repo)

def get_domain_service(
    domain_repo: DomainRepo = Depends(get_domain_repository),
    enrichment_service: EnrichmentService = Depends(get_enrichment_service),
    db = Depends(get_db)
) -> DomainService:
    return DomainService(domain_repo, enrichment_service, db)

def get_lexical_service(repo: LexicalRepo = Depends(get_lexical_repository)):
    return LexicalService(repo)

def get_list_repository(session: AsyncSession = Depends(get_async_session)) -> ListRepo:
    return ListRepo(session)

def get_list_service(
    list_repo: ListRepo = Depends(get_list_repository) 
) -> ListService:
    return ListService(list_repo)

def get_lexical_enricher(_lexical_service: LexicalService = Depends(get_lexical_service), _job_service: EnrichmentService = Depends(get_enrichment_service), db: Database = Depends(get_db)):
    return LexicalEnricher(_lexical_service, _job_service, db)

def get_rdap_service(db: Database = Depends(get_db)):
    return RDAPService(db)

def get_rdap_enricher(db: Database = Depends(get_db)):
    return RDAPEnricher(db)