"""Tests for the item sales-order quantity discovery contract."""

from tap_dynamics_bc.streams import QtyOnSalesOrderStream
from tap_dynamics_bc.tap import STREAM_TYPES


def test_qty_on_sales_order_stream_is_discoverable_as_full_sync():
    """Expose current item sales-order demand as a company-scoped full sync."""
    assert QtyOnSalesOrderStream in STREAM_TYPES
    assert QtyOnSalesOrderStream.name == "qty_on_sales_order"
    assert QtyOnSalesOrderStream.path == "/Artikel"
    assert QtyOnSalesOrderStream.primary_keys == ["No", "company_id"]
    assert QtyOnSalesOrderStream.replication_key is None
    assert QtyOnSalesOrderStream.select == "No,Qty_on_Sales_Order"

    assert set(QtyOnSalesOrderStream.schema["properties"]) == {
        "No",
        "Qty_on_Sales_Order",
        "company_id",
        "company_name",
    }
