from fastapi import FastAPI
from enrichmentcenter.api.routes import enrichment

app = FastAPI()

app.include_router(enrichment.router)