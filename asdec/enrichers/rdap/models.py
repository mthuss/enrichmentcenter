from sqlalchemy import Boolean, Date, Float, ForeignKey, String, Integer, UniqueConstraint, Index
from sqlalchemy.orm import mapped_column, Mapped, relationship
from asdec.core.db import Base
from pydantic import BaseModel as PydanticBase
from datetime import date

class RegistrationFeatures(Base):
    __tablename__ = "registration"

    id: Mapped[int] = mapped_column(primary_key=True)
    
    # TODO: add lifetime = registration_date - today as a feature and maybe others
    available: Mapped[Boolean] = mapped_column(Boolean(), nullable=False)
    registrable_domain: Mapped[str] = mapped_column(String(length=256), nullable=False, unique=True)
    registrar_handle: Mapped[str] = mapped_column(String(length=16), nullable=True)
    registrar_name: Mapped[String] = mapped_column(String(length=256), nullable=True)
    registration_date: Mapped[Date] = mapped_column(Date(), nullable=True)
    last_updated: Mapped[Date] = mapped_column(Date(), nullable=True)
    expiration_date: Mapped[Date] = mapped_column(Date(), nullable=True)
    age_days: Mapped[Integer] = mapped_column(Integer(), nullable=True)
    registrar_country: Mapped[String] = mapped_column(String(length=5), nullable=True)
    dnssec: Mapped[Boolean] = mapped_column(Boolean(), nullable=True)
    status_flags: Mapped[list["RegistrationStatusFlags"]] = relationship(
        back_populates="registration",
        cascade="all, delete-orphan",
    )
    all_features: Mapped[Boolean] = mapped_column(Boolean(), nullable=False)
    domains: Mapped[list["DomainRegistration"]] = relationship(
        back_populates="registration",
        cascade="all, delete-orphan"
    )

class RegistrationStatusFlags(Base):
    __tablename__ = "registration_statusflags"
    __table_args__ = (
        UniqueConstraint(
            "registration_id",
            "flag",
            name="uq_registration_flag",
        ),
        Index("ix_registration_status_flags_flag", "flag"),
    )

    registration_id: Mapped[int] = mapped_column(ForeignKey("registration.id", on_delete="CASCADE"), primary_key=True)
    flag: Mapped[String] = mapped_column(String(length=128), primary_key=True)
    registration: Mapped["RegistrationFeatures"] = relationship(
        back_populates="status_flags",
    )

class RegistrationStatusFlagsCreate(PydanticBase):
    registration_id: int
    flag: str

class RegistrationFeaturesCreate(PydanticBase):
    available: bool
    registrable_domain: str
    registrar_handle: str | None
    registrar_name: str | None
    registration_date: date | None
    last_updated: date | None
    expiration_date: date | None
    age_days: int | None
    registrar_country: str | None
    dnssec: bool | None
    status_flags: list[str] | None
    all_features: bool

class DomainRegistration(Base):
    __tablename__ = "rel_domain_registration"
    __table_args__ = (
        UniqueConstraint(
            "domain_id",
            "registration_id",
            name="uq_domain_registration",
        ),
    )
    domain_id: Mapped[int] = mapped_column(
        ForeignKey("domains.id"),
        primary_key=True,
        nullable=False
    )
    registration_id: Mapped[int] = mapped_column(
        ForeignKey("registration.id"),
        nullable=False
    )
    registration: Mapped["RegistrationFeatures"] = relationship(back_populates="domains")
