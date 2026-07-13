"""Tests for the item units-of-measure discovery contract."""

from tap_dynamics_bc.streams import ItemsDetailsStream, ItemUnitsOfMeasureStream
from tap_dynamics_bc.tap import STREAM_TYPES


def test_item_units_of_measure_stream_is_discoverable_with_conversion_fields():
    """Expose item UoM conversion factors as a company-scoped Singer stream."""
    assert ItemUnitsOfMeasureStream in STREAM_TYPES
    assert ItemUnitsOfMeasureStream.name == "item_units_of_measure"
    assert ItemUnitsOfMeasureStream.path == "/Artikeleenheden"
    assert ItemUnitsOfMeasureStream.primary_keys == ["Item_No", "Code", "company_id"]
    assert ItemUnitsOfMeasureStream.replication_key is None

    properties = ItemUnitsOfMeasureStream.schema["properties"]
    assert set(properties) == {
        "Item_No",
        "Code",
        "Qty_per_Unit_of_Measure",
        "TINX_Is_Default",
        "XPRT_Qty_Rounding_Precision",
        "ItemUnitOfMeasure",
        "ItemBaseUOMQtyPrecision",
        "company_id",
        "company_name",
    }


def test_item_details_exposes_base_sales_and_purchase_units_of_measure():
    """Expose item UoM settings independently of supplier-product filtering."""
    properties = ItemsDetailsStream.schema["properties"]

    expected_uom_fields = {
        "Base_Unit_of_Measure",
        "Sales_Unit_of_Measure",
        "Purch_Unit_of_Measure",
    }
    assert expected_uom_fields.issubset(ItemsDetailsStream.select.split(","))
    assert expected_uom_fields.issubset(properties)
