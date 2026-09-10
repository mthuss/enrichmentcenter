from contextlib import asynccontextmanager
import logging
import uvicorn
from fastapi import FastAPI
from asdec.api.routes import enrichment
from asdec.core.db import Database
from asdec.core.config import Settings
from asdec.enrichers.lists.service import ListService
from asdec.enrichers.lists.repo import ListRepo
import asyncio

@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    db = Database(settings)
    await db.createTables()
    app.state.db = db
    async with db.getSession() as session:
        await ListService(ListRepo(session)).populateFeeds()

    yield

    await app.state.db.engine.dispose()

app = FastAPI(lifespan=lifespan)

app.include_router(enrichment.router)

if __name__ == "__main__":

    uvicorn.run(
        "main:app", host="0.0.0.0", 
        reload=False, port=8000
    )