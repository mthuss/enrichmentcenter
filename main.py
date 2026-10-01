from contextlib import asynccontextmanager
import asdec.core.logging
import uvicorn
from fastapi import FastAPI
from asdec.api.routes import enrichment
from asdec.core.db import Database
from asdec.core.config import Settings
from asdec.enrichers.lists.service import ListService
from asdec.enrichers.lists.repo import ListRepo
import asyncio

from asdec.enrichment.repo import EnrichmentJobRepo
from asdec.enrichment.service import EnrichmentService

@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    db = Database(settings)
    await db.createTables()
    app.state.db = db
    async with db.getSession() as session:
        await ListService(ListRepo(session)).populateFeeds()
        await EnrichmentService(EnrichmentJobRepo(session)).reschedule_broken_jobs_on_startup()

    # add a function to check orphan 
    # enrichment processes and 
    # reschedule them here

    yield

    await app.state.db.engine.dispose()

app = FastAPI(lifespan=lifespan)

app.include_router(enrichment.router)

if __name__ == "__main__":

    uvicorn.run(
        "main:app", host="0.0.0.0", 
        reload=False, port=8000
    )