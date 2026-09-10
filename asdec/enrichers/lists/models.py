# Database functions
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import func, DateTime
from datetime import date, datetime
from asdec.core.db import Base
from pydantic import BaseModel as PydanticBase

from sqlalchemy import (
    Boolean,
    Date,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)

class Feed(Base):
    __tablename__ = "feeds"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(length=255), nullable=False)
    url: Mapped[str] = mapped_column(String(length=2048), nullable=False, unique=True)
    parser: Mapped[str] = mapped_column(String(length=32))
    abuse_type: Mapped[str] = mapped_column(String(length=64), nullable=True)
    parse_char: Mapped[str] = mapped_column(String(length=7), nullable=True)
    csv_column: Mapped[str] = mapped_column(String(length=32))
    malicious: Mapped[bool] = mapped_column(Boolean(),nullable=False)
    creation_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    entries: Mapped[list["FeedDomain"]] = relationship(
        back_populates="feed",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return self.name

class FeedCreate(PydanticBase):
    name: str
    url: str
    parser: str = "#"
    abuse_type: str | None
    parse_char: str
    csv_column: str | None
    malicious: bool

class Domain(Base):
    __tablename__ = "domains"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(length=255), nullable=False, unique=True)
    feed_entries: Mapped[list["FeedDomain"]] = relationship(
        back_populates="domain",
        cascade="all, delete-orphan"
    )

class DomainCreate(PydanticBase):
    name: str

class FeedDomain(Base):
    __tablename__ = "rel_feed_domain"
    __table_args__ = (
        UniqueConstraint(
            "domain_id",
            "feed_id",
            name="uq_domain_blacklist",
        ),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    domain_id: Mapped[int] = mapped_column(
        ForeignKey("domains.id"),
        nullable=False
    )
    feed_id: Mapped[int] = mapped_column(
        ForeignKey("feeds.id"),
        nullable=False
    )
    domain: Mapped["Domain"] = relationship(back_populates="feed_entries")
    feed: Mapped["Feed"] = relationship(back_populates="entries")
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
