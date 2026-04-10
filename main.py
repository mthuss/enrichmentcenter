from fastapi import FastAPI
from app.api.routes import enrichment

app = FastAPI()

app.include_router(enrichment.router)