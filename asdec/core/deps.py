from asdec.enrichers.lists.service import DomainService, ListService
from asdec.enrichers.lists.repo import DomainRepo, ListRepo
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends, Request
from asdec.core.db import Database
from asdec.core.config import Settings

def get_db(request: Request) -> Database:
    return request.app.state.db

def getSettings():
    return Settings()

async def get_async_session(db: Database = Depends(get_db)):
    async with db.getSession() as session:
        yield session

def get_domain_repository(session: AsyncSession = Depends(get_async_session)) -> DomainRepo:
    return DomainRepo(session)

def get_domain_service(
    domain_repo: DomainRepo = Depends(get_domain_repository) 
) -> DomainService:
    return DomainService(domain_repo)

def get_list_repository(session: AsyncSession = Depends(get_async_session)) -> ListRepo:
    return ListRepo(session)

def get_list_service(
    list_repo: ListRepo = Depends(get_list_repository) 
) -> ListService:
    return ListService(list_repo)