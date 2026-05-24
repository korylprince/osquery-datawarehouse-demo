package trino

import "testing"

func TestParseTableRef(t *testing.T) {
	ref, err := ParseTableRef("sales.orders", "iceberg", "default")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ref.Catalog != "iceberg" || ref.Schema != "sales" || ref.Table != "orders" {
		t.Fatalf("unexpected ref: %#v", ref)
	}
	if _, err := ParseTableRef("bad-name.orders", "iceberg", "default"); err == nil {
		t.Fatal("expected invalid identifier error")
	}
}
