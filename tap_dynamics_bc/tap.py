"""dynamics-bc tap class."""

import datetime
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_dynamics_bc.streams import (
    AccountsStream,
    BOMComponentsStream,
    CompaniesStream,
    CompanyInformationStream,
    ItemsDetailsStream,
    ItemsStream,
    LocationsStream,
    PurchaseInvoicesStream,
    PurchaseOrdersStream,
    PurchaseReceiptsStream,
    SalesInvoicesStream,
    SupplierProductsStream,
    VendorPurchases,
    VendorsStream,
    SalesOrdersStream,
    GeneralLedgerEntriesStream,
    GeneralLedgerEntriesIncrementalStream,
    GLEntriesDimensionsStream,
    DimensionsStream,
    DimensionValuesStream,
    CustomersStream,
    CurrenciesStream,
    VendorPaymentJournalsStream,
    PaymentTermsStream,
    VendorLedgerEntriesStream,
    ItemVariantsStream,
    InventoryByLocationStream,
    ItemWithVariantsStream,
)

STREAM_TYPES = [
    CompaniesStream,
    CompanyInformationStream,
    ItemsStream,
    ItemsDetailsStream,
    VendorsStream,
    VendorPurchases,
    SalesInvoicesStream,
    PurchaseInvoicesStream,
    PurchaseOrdersStream,
    PurchaseReceiptsStream,
    SupplierProductsStream,
    AccountsStream,
    LocationsStream,
    SalesOrdersStream,
    GeneralLedgerEntriesStream,
    GeneralLedgerEntriesIncrementalStream,
    GLEntriesDimensionsStream,
    DimensionsStream,
    DimensionValuesStream,
    CustomersStream,
    CurrenciesStream,
    VendorPaymentJournalsStream,
    PaymentTermsStream,
    VendorLedgerEntriesStream,
    ItemVariantsStream,
    InventoryByLocationStream,
    ItemWithVariantsStream,
    BOMComponentsStream,
]


class TapdynamicsBc(Tap):
    """dynamics-bc tap class."""

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, catalog, state, parse_env_config, validate_config)
        self.state["full_sync_purchase_orders"] = self._is_new_hotglue_day()

    name = "tap-dynamics-bc"

    def _is_new_hotglue_day(self) -> bool:
        """Return true when the previous Hotglue job state is before today."""
        hg_last_modified = self.state.get("hg_last_modified")
        if not hg_last_modified:
            return False

        try:
            last_hotglue_run = datetime.datetime.fromisoformat(
                str(hg_last_modified).replace("Z", "+00:00")
            )
        except ValueError:
            self.logger.warning(
                "Could not parse hg_last_modified value %s; setting full_sync_purchase_orders to false",
                hg_last_modified,
            )
            return False

        if last_hotglue_run.tzinfo is None:
            last_hotglue_run = last_hotglue_run.replace(tzinfo=datetime.timezone.utc)

        current_date = datetime.datetime.now(datetime.timezone.utc).date()
        previous_hotglue_date = last_hotglue_run.astimezone(
            datetime.timezone.utc
        ).date()
        return previous_hotglue_date < current_date

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=False,
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "environment_name",
            th.StringType,
            required=True,
        ),
        th.Property(
            "company_ids",
            th.StringType,
            required=False,
            description="Optional company ID(s) to sync. Can be a single company ID string or comma-separated company IDs. If not provided, all companies will be synced.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapdynamicsBc.cli()
