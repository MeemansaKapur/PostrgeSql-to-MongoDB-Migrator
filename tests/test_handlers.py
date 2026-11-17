from bson.decimal128 import Decimal128
from pymongo import DeleteOne, UpdateMany, UpdateOne

from migrator.cdc.handlers import Event, Handlers


def ev(table, op, before=None, after=None):
    return Event(f"bank.public.{table}", 0, 0, table, op, before, after, 0)


def doc(op):
    return op._doc


ADDR = {"address_id": 5001, "customer_id": 1042, "type": "home", "line1": "x", "city": "Pune",
        "state": "MH", "pincode": "411001"}
CARD = {"card_id": 8001, "account_id": 3001, "last4": "4821", "network": "rupay",
        "expires_on": 20454, "status": "active"}
ACCT = {"account_id": 3001, "account_no": "100200300400", "type_id": 1, "branch_id": 7,
        "balance": "45210.50", "status": "active", "opened_at": "2024-01-09T10:22:31.512345Z"}
LOOKUPS = {
    "account_types": {1: {"type_id": 1, "code": "SAV", "name": "Savings", "interest_rate": "3.50",
                          "min_balance": "1000.00"}},
    "branches": {7: {"branch_id": 7, "code": "PUN07", "name": "Pune", "city": "Pune",
                     "ifsc": "BANK0000007", "opened_on": 11000}},
}


def test_root_upsert_sets_absolute_state(mapping):
    [(coll, op)] = Handlers(mapping, dict(LOOKUPS)).ops_for(ev("accounts", "u", None, ACCT))
    assert coll == "accounts" and isinstance(op, UpdateOne)
    assert op._filter == {"_id": 3001} and op._upsert
    s = op._doc[0]["$set"]
    assert s["balance"] == {"$literal": Decimal128("45210.50")} and s["_has_row"] == {"$literal": True}
    assert s["type"]["$literal"]["code"] == "SAV" and s["branch"]["$literal"]["ifsc"] == "BANK0000007"
    assert "type_id" not in s
    # arrays owned by other topics are kept, only created if missing
    assert s["cards"] == {"$ifNull": ["$cards", []]} and s["holders"] == {"$ifNull": ["$holders", []]}


def test_root_delete(mapping):
    [(coll, op)] = Handlers(mapping).ops_for(ev("customers", "d", {"customer_id": 9}, None))
    assert coll == "customers" and isinstance(op, DeleteOne) and op._filter == {"_id": 9}


def test_embedded_upsert_is_one_atomic_pipeline_with_literal(mapping):
    [(coll, op)] = Handlers(mapping).ops_for(ev("addresses", "c", None, {**ADDR, "line1": "$500 refund"}))
    assert coll == "customers" and op._filter == {"_id": 1042} and op._upsert
    pipeline = op._doc
    concat = pipeline[0]["$set"]["addresses"]["$concatArrays"]
    assert concat[1] == [{"$literal": {k: v for k, v in ADDR.items() if k != "customer_id"} | {"line1": "$500 refund"}}]


def test_embedded_delete_uses_old_fk(mapping):
    [(coll, op)] = Handlers(mapping).ops_for(ev("addresses", "d", ADDR, None))
    assert op._filter == {"_id": 1042} and op._doc == {"$pull": {"addresses": {"address_id": 5001}}}


def test_embedded_delete_without_full_identity_finds_parent_by_multikey(mapping):
    [(coll, op)] = Handlers(mapping).ops_for(ev("cards", "d", {"card_id": 8001}, None))
    assert op._filter == {"cards.card_id": 8001}


def test_child_moved_to_another_parent(mapping):
    ops = Handlers(mapping).ops_for(ev("cards", "u", CARD, {**CARD, "account_id": 3002}))
    assert [o._filter for _, o in ops] == [{"_id": 3001}, {"_id": 3002}]
    assert ops[0][1]._doc == {"$pull": {"cards": {"card_id": 8001}}}


def test_many_to_many_both_sides(mapping):
    row = {"account_id": 3001, "customer_id": 1042, "role": "joint"}
    ops = dict(Handlers(mapping).ops_for(ev("account_holders", "c", None, row)))
    assert ops["customers"]._doc == {"$addToSet": {"account_ids": 3001}}
    assert ops["accounts"]._filter == {"_id": 3001}
    dels = dict(Handlers(mapping).ops_for(ev("account_holders", "d", row, None)))
    assert dels["customers"]._doc == {"$pull": {"account_ids": 3001}}
    assert dels["accounts"]._doc == {"$pull": {"holders": {"customer_id": 1042}}}


def test_lookup_change_fans_out_and_updates_cache(mapping):
    h = Handlers(mapping, {t: dict(v) for t, v in LOOKUPS.items()})
    new = {**LOOKUPS["account_types"][1], "interest_rate": "4.00"}
    [(coll, op)] = h.ops_for(ev("account_types", "u", None, new))
    assert coll == "accounts" and isinstance(op, UpdateMany)
    assert op._filter == {"type.type_id": 1}
    assert op._doc["$set"]["type.interest_rate"] == Decimal128("4.00")
    assert h.lookups["account_types"][1]["interest_rate"] == "4.00"


def test_branch_is_both_root_and_lookup(mapping):
    ops = Handlers(mapping, dict(LOOKUPS)).ops_for(ev("branches", "u", None, LOOKUPS["branches"][7]))
    assert sorted(c for c, _ in ops) == ["accounts", "branches"]


def test_canary_produces_no_writes(mapping):
    assert Handlers(mapping).ops_for(ev("canary", "u", None, {"id": 1, "written_ms": 1})) == []


def test_dropped_column_is_schema_drift(mapping):
    import pytest

    from migrator.cdc.handlers import SchemaDrift
    bad = {k: v for k, v in ACCT.items() if k != "status"}
    with pytest.raises(SchemaDrift):
        Handlers(mapping, dict(LOOKUPS)).ops_for(ev("accounts", "u", None, bad))
