from migrator.schema.introspect import Catalog, ForeignKey, Table
from migrator.schema.propose import classify, propose


def _cat():
    t = {
        "customers": Table("customers", {"customer_id": "integer", "name": "text"}, ["customer_id"], [], 10000),
        "addresses": Table("addresses", {"address_id": "integer", "customer_id": "integer"}, ["address_id"], [], 15000),
        "types": Table("types", {"type_id": "integer", "code": "text", "name": "text", "rate": "numeric",
                                 "extra": "text"}, ["type_id"], ["code"], 4),
        "accounts": Table("accounts", {"account_id": "integer", "type_id": "integer"}, ["account_id"], [], 14000),
        "holders": Table("holders", {"account_id": "integer", "customer_id": "integer", "role": "text"},
                         ["account_id", "customer_id"], [], 16000),
        "cards": Table("cards", {"card_id": "integer", "account_id": "integer"}, ["card_id"], [], 18000),
        "card_events": Table("card_events", {"id": "integer", "card_id": "integer"}, ["id"], [], 100),
        "txns": Table("txns", {"txn_id": "bigint", "account_id": "integer"}, ["txn_id"], [], 150000),
        "canary": Table("canary", {"id": "smallint"}, ["id"], [], 1),
    }
    fks = [
        ForeignKey("addresses", "customer_id", "customers", "customer_id", 2, 1.5),
        ForeignKey("accounts", "type_id", "types", "type_id", 9000, 3500),
        ForeignKey("holders", "account_id", "accounts", "account_id", 2, 1.1),
        ForeignKey("holders", "customer_id", "customers", "customer_id", 4, 1.6),
        ForeignKey("cards", "account_id", "accounts", "account_id", 3, 1.2),
        ForeignKey("card_events", "card_id", "cards", "card_id", 5, 1.0),
        ForeignKey("txns", "account_id", "accounts", "account_id", 412, 10),
    ]
    return Catalog(t, fks)


def test_classification_rules():
    k = classify(_cat(), {"canary"})
    assert k["holders"].kind == "join"
    assert k["types"].kind == "lookup"
    assert k["addresses"].kind == "embed"
    assert k["cards"].kind == "root", "a child with its own children must not be embedded"
    assert k["card_events"].kind == "embed"
    assert k["txns"].kind == "root" and "412" in k["txns"].reason
    assert "canary" not in k


def test_embed_threshold_is_tunable():
    assert classify(_cat(), set(), embed_max=500)["txns"].kind == "embed"


def test_proposed_mapping_shape():
    m = propose(_cat(), {"canary"})
    c = m["collections"]
    assert set(c) == {"customers", "accounts", "cards", "txns"}   # lookup 'types' has no collection
    assert c["customers"]["embed"][0]["table"] == "addresses"
    ref = c["accounts"]["extended_refs"][0]
    assert ref["as"] == "type" and ref["fields"] == ["type_id", "code", "name", "rate"]
    assert {s["via"] for s in c["accounts"]["many_to_many"]} == {"holders"}
    assert c["customers"]["many_to_many"][0]["keep_fields"] == ["account_id", "role"]
    assert c["txns"]["references"][0] == {"field": "account_id", "collection": "accounts",
                                          "reason": "plain id reference"}
