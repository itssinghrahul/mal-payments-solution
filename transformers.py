"""
transformers.py — Squad-specific CSV -> Canonical schema transformers
"""
from __future__ import annotations
import pandas as pd
from datetime import datetime, timezone
from schema import PaymentEventV1, migrate_v1_to_v2, make_event_id

_CARD_STATUS     = {"APPROVED":"APPROVED","DECLINED":"DECLINED","PENDING":"PENDING"}
_TRANSFER_STATUS = {"COMPLETED":"COMPLETED","PENDING":"PENDING","FAILED":"FAILED"}
_BILL_STATUS     = {"SUCCESS":"APPROVED","FAILED":"FAILED","PENDING":"PENDING"}

def transform_cards(df):
    events, errors = [], []
    for _, row in df.iterrows():
        try:
            ts = datetime.strptime(f"{row['txn_date']} {row['txn_time']}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            ev = PaymentEventV1(
                event_id=make_event_id("cards", row["txn_id"], ts),
                payment_type="CARD", customer_id=str(row["cust_id"]),
                amount=round(float(row["amount"]), 2), currency=str(row["ccy"]).upper(),
                status=_CARD_STATUS.get(str(row["status"]).upper(), "PENDING"),
                event_timestamp=ts, payment_method=str(row["card_type"]),
                source_system="cards", raw_reference=str(row["txn_id"]),
                metadata={"merchant_name": row.get("merchant_name"),
                          "merchant_category": row.get("merchant_category"),
                          "auth_code": row.get("auth_code"),
                          "masked_card": f"****{str(row['card_number'])[-4:]}"},
            )
            errs = ev.validate()
            if errs:
                errors.append({"source":"cards","row":row.to_dict(),"error":errs})
            else:
                events.append(migrate_v1_to_v2(ev, counterparty_name=str(row.get("merchant_name",""))))
        except Exception as exc:
            errors.append({"source":"cards","row":row.to_dict(),"error":str(exc)})
    return events, errors

def transform_transfers(df):
    events, errors = [], []
    for _, row in df.iterrows():
        try:
            ts = datetime.fromisoformat(str(row["initiated_at"]).replace("Z","+00:00"))
            ev = PaymentEventV1(
                event_id=make_event_id("transfers", row["transfer_ref"], ts),
                payment_type="TRANSFER", customer_id=str(row["initiator_customer"]),
                amount=round(float(row["transfer_amt"]), 2), currency=str(row["transfer_ccy"]).upper(),
                status=_TRANSFER_STATUS.get(str(row["transfer_status"]).upper(), "PENDING"),
                event_timestamp=ts, payment_method=str(row["transfer_type"]),
                source_system="transfers", raw_reference=str(row["transfer_ref"]),
                metadata={"sender_account": row.get("sender_account"),
                          "receiver_account": row.get("receiver_account"),
                          "receiver_bank": row.get("receiver_bank"),
                          "failure_reason": row.get("failure_reason")},
            )
            errs = ev.validate()
            if errs:
                errors.append({"source":"transfers","row":row.to_dict(),"error":errs})
            else:
                fee = float(row.get("fee_amt", 0) or 0)
                events.append(migrate_v1_to_v2(ev, fee_amount=fee,
                    fee_currency=str(row.get("fee_ccy", ev.currency)),
                    counterparty_name=str(row.get("receiver_name",""))))
        except Exception as exc:
            errors.append({"source":"transfers","row":row.to_dict(),"error":str(exc)})
    return events, errors

def transform_bills(df):
    events, errors = [], []
    for _, row in df.iterrows():
        try:
            ts = datetime.strptime(str(row["payment_datetime"]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            ev = PaymentEventV1(
                event_id=make_event_id("bill_payments", row["bill_pay_id"], ts),
                payment_type="BILL", customer_id=str(row["customer_no"]),
                amount=round(float(row["payment_amount"]), 2), currency=str(row["payment_currency"]).upper(),
                status=_BILL_STATUS.get(str(row["payment_result"]).upper(), "PENDING"),
                event_timestamp=ts, payment_method=str(row["payment_channel"]),
                source_system="bill_payments", raw_reference=str(row["bill_pay_id"]),
                metadata={"biller_code": row.get("biller_code"),
                          "biller_name": row.get("biller_name"),
                          "biller_category": row.get("biller_category"),
                          "account_ref": row.get("account_ref"),
                          "error_code": row.get("error_code"),
                          "scheduled": row.get("scheduled_flag") == "Y"},
            )
            errs = ev.validate()
            if errs:
                errors.append({"source":"bill_payments","row":row.to_dict(),"error":errs})
            else:
                events.append(migrate_v1_to_v2(ev, counterparty_name=str(row.get("biller_name",""))))
        except Exception as exc:
            errors.append({"source":"bill_payments","row":row.to_dict(),"error":str(exc)})
    return events, errors
