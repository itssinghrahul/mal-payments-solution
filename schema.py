"""
schema.py — Canonical Payment Event Schema (v1 + v2)
Pure stdlib + dataclasses. No third-party dependencies.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
import hashlib

PAYMENT_TYPES  = {"CARD", "TRANSFER", "BILL"}
VALID_STATUSES = {"APPROVED", "DECLINED", "PENDING", "FAILED", "COMPLETED"}
SCHEMA_V1      = "1.0"
SCHEMA_V2      = "2.0"

@dataclass
class PaymentEventV1:
    event_id        : str
    payment_type    : str
    customer_id     : str
    amount          : float
    currency        : str
    status          : str
    event_timestamp : datetime
    payment_method  : str
    source_system   : str
    raw_reference   : str
    metadata        : dict = field(default_factory=dict)
    schema_version  : str  = SCHEMA_V1

    def validate(self) -> list:
        errors = []
        if self.payment_type not in PAYMENT_TYPES:
            errors.append(f"Invalid payment_type '{self.payment_type}'")
        if self.status not in VALID_STATUSES:
            errors.append(f"Invalid status '{self.status}'")
        if self.amount < 0:
            errors.append(f"amount must be non-negative, got {self.amount}")
        if not self.customer_id or not str(self.customer_id).strip():
            errors.append("customer_id cannot be empty")
        if len(str(self.currency)) != 3:
            errors.append(f"currency must be 3-char ISO-4217, got '{self.currency}'")
        return errors

@dataclass
class PaymentEventV2(PaymentEventV1):
    fee_amount        : float = 0.0
    fee_currency      : Optional[str] = None
    counterparty_name : Optional[str] = None
    schema_version    : str  = SCHEMA_V2

    def validate(self) -> list:
        errors = super().validate()
        if self.fee_amount < 0:
            errors.append(f"fee_amount must be non-negative, got {self.fee_amount}")
        return errors

def migrate_v1_to_v2(event, fee_amount=0.0, fee_currency=None, counterparty_name=None):
    return PaymentEventV2(
        event_id=event.event_id, payment_type=event.payment_type,
        customer_id=event.customer_id, amount=event.amount,
        currency=event.currency, status=event.status,
        event_timestamp=event.event_timestamp, payment_method=event.payment_method,
        source_system=event.source_system, raw_reference=event.raw_reference,
        metadata=event.metadata, fee_amount=round(fee_amount, 2),
        fee_currency=fee_currency or event.currency,
        counterparty_name=counterparty_name,
    )

def make_event_id(source_system, raw_reference, timestamp):
    key = f"{source_system}::{raw_reference}::{timestamp.isoformat()}"
    return hashlib.sha256(key.encode()).hexdigest()[:24]

def event_to_dict(event):
    d = asdict(event)
    d["event_timestamp"] = event.event_timestamp.isoformat()
    return d
