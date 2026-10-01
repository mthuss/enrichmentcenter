from asdec.enrichers.lexical.models import LexicalFeaturesCreate
from typing import Any
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import enum
from asdec.core.db import Base
from pydantic import BaseModel as PydanticBase
from sqlalchemy import (
    Date,
    ForeignKey,
    Integer,
    func,
    Enum, Boolean,
)

class EnrichmentType(enum.Enum):
    LEXICAL = "lexical"
    RDAP = "rdap"
    PDNS = "pdns"

class EnrichmentError(enum.Enum):
    TIMED_OUT = "timed_out"
    PROCESSING_ERROR = "processing_error"

class EnrichmentStatus(enum.Enum):
    PENDING = 0
    COMPLETE = 1
    IN_PROGRESS = 2
    TIMED_OUT = 3
    ERRORED = 4

class JobInfo:
    job: EnrichmentJob
    domain_name: str

    def __init__(self, job:EnrichmentJob, domain_name: str):
        self.job = job
        self.domain_name = domain_name

class EnrichmentException(Exception):
    def __init__(self, code: EnrichmentError):
        self.code = code
        super().__init__(code.value)

class EnrichmentResults:
    job_id: int
    domain_id: int
    results: LexicalFeaturesCreate
    error: EnrichmentError | None

    def __init__(self, job_id: int, domain_id: int, results: Any, error: EnrichmentError | None):
        self.job_id = job_id
        self.domain_id = domain_id
        self.results = results
        self.error = error

ENRICHMENT_INTERVALS = {
    EnrichmentType.LEXICAL: None,
    EnrichmentType.RDAP: 7,
    EnrichmentType.PDNS: 1,
}

# -----------------------------------
# Class for the enrichment job queue
# -----------------------------------
class EnrichmentJob(Base):
    __tablename__ = "enrichment_job"
    id: Mapped[int] = mapped_column(primary_key=True)
    domain: Mapped[int] = mapped_column(
        ForeignKey("domains.id"),
        nullable=False
    )
    enrichment_type: Mapped[EnrichmentType] = mapped_column(
        Enum(EnrichmentType),
        nullable=False
    )
    next_enrichment: Mapped[Date] = mapped_column(Date(), server_default=func.now(), nullable=True)
    status: Mapped[EnrichmentStatus] = mapped_column(Enum(EnrichmentStatus), nullable=False) 

class EnrichmentJobCreate(PydanticBase):
    domain: int
    enrichment_type: EnrichmentType
    status: EnrichmentStatus