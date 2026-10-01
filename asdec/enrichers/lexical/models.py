from asdec.core.db import Base
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import ForeignKey, String, Integer, Float
from pydantic import BaseModel as PydanticBase

class LexicalFeatures(Base):
    __tablename__ = "lexical"
    id: Mapped[int] = mapped_column(
        ForeignKey("domains.id"),
        primary_key=True
    )
    tld: Mapped[str] = mapped_column(String(length=63), nullable=False)
    sld: Mapped[str] = mapped_column(String(length=63), nullable=True)
    suffix: Mapped[str] = mapped_column(String(length=128), nullable=False)
    length: Mapped[int] = mapped_column(Integer(), nullable=False)
    digits: Mapped[int] = mapped_column(Integer(), nullable=False)
    digit_ratio: Mapped[float] = mapped_column(Float(), nullable=False)
    hyphen_count: Mapped[int] = mapped_column(Integer(), nullable=False)
    label_count: Mapped[int] = mapped_column(Integer(), nullable=False)
    shannon_entropy: Mapped[float] = mapped_column(Float(), nullable=False)

class LexicalFeaturesCreate(PydanticBase):
    id: int
    tld: str
    sld: str | None
    suffix: str
    length: int
    digits: int
    digit_ratio: float
    hyphen_count: int
    label_count: int
    shannon_entropy: float