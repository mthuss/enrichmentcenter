from sqlalchemy import Boolean, Date, ForeignKey, String, Integer, UniqueConstraint, Index
from sqlalchemy.orm import mapped_column, Mapped, relationship
from asdec.core.db import Base
from pydantic import BaseModel as PydanticBase
from datetime import date

class RegistrationFeatures(Base):
    __tablename__ = "registration"

    domain_id: Mapped[int] = mapped_column(
        ForeignKey("domains.id"),
        primary_key=True
    )

    registrar_handle: Mapped[int] = mapped_column(Integer(), nullable=True)
    registrar_name: Mapped[String] = mapped_column(String(length=256), nullable=True)
    registration_date: Mapped[Date] = mapped_column(Date(), nullable=True)
    last_updated: Mapped[Date] = mapped_column(Date(), nullable=True)
    expiration_date: Mapped[Date] = mapped_column(Date(), nullable=True)
    registrar_country: Mapped[String] = mapped_column(String(length=5), nullable=True)
    dnssec: Mapped[Boolean] = mapped_column(Boolean(), nullable=True)
    status_flags: Mapped[list["RegistrationStatusFlags"]] = relationship(
        back_populates="registration",
        cascade="all, delete-orphan",
    )

class RegistrationStatusFlags(Base):
    __tablename__ = "registration_statusflags"
    __table_args__ = (
        UniqueConstraint(
            "domain",
            "flag",
            name="uq_registration_flag",
        ),
        Index("ix_registration_status_flags_flag", "flag"),
    )

    domain: Mapped[int] = mapped_column(ForeignKey("registration.domain_id", on_delete="CASCADE"), primary_key=True)
    flag: Mapped[String] = mapped_column(String(length=64), primary_key=True)
    registration: Mapped["RegistrationFeatures"] = relationship(
        back_populates="status_flags",
    )

class RegistrationStatusFlagsCreate(PydanticBase):
    domain: int
    flag: str

class RegistrationFeaturesCreate(PydanticBase):
    domain_id: int
    registrar_handle: str | None
    registrar_name: str | None
    registration_date: date | None
    last_updated: date | None
    expiration_date: date | None
    registrar_country: str | None
    dnssec: bool | None
    status_flags: list[RegistrationStatusFlagsCreate] | None
