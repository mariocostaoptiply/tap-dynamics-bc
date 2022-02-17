"""dynamics-bc tap class."""

from typing import List
import requests

from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from tap_dynamics_bc.streams import (
    CompaniesStream,
    CompanyInformationStream,
    ItemsStream,
    VendorsStream,
    VendorPurchases,
    SalesInvoicesStream,
    PurchaseInvoicesStream,
)

STREAM_TYPES = [
    CompaniesStream,
    CompanyInformationStream,
    ItemsStream,
    VendorsStream,
    VendorPurchases,
    SalesInvoicesStream,
    PurchaseInvoicesStream,
]


class TapdynamicsBc(Tap):
    """dynamics-bc tap class."""

    name = "tap-dynamics-bc"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
        ),
        th.Property(
            "access_key",
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
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapdynamicsBc.cli()
