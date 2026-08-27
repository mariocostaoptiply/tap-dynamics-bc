"""REST client handling, including dynamics-bcStream base class."""

from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

import requests
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath
from hotglue_singer_sdk.streams import RESTStream

from tap_dynamics_bc.auth import TapDynamicsBCAuth
from backports.cached_property import cached_property
import copy
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
import singer
from singer import StateMessage

# Business Central stamps unmodified records with this sentinel timestamp on
# system audit fields (e.g. SystemModifiedAt, lastModifiedDateTime). Such
# records are older than any real start_date and would be dropped by a plain
# "greater than" replication filter, so initial syncs must explicitly keep them.
BC_DEFAULT_MODIFIED_SENTINEL = "0001-01-01T00:00:00Z"


class dynamicsBcStream(RESTStream):
    """dynamics-bc stream class."""

    envs_list = None
    page_size = 5000  # 20,000 is the Dynamics BC maximum and default size
    timeout = 600  # 10 minutes (same as Dynamics BC API)

    def get_environment(self):
        env_name = self.config.get("environment_name", "production")
        if "?" in env_name:
            env_name = env_name.split("?")
            if isinstance(env_name, list):
                env_name = env_name[0]
        self.validate_env(env_name)
        return env_name

    @cached_property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        url_template = "https://api.businesscentral.dynamics.com/v2.0/{}/api/v2.0"
        return url_template.format(self.get_environment())

    records_jsonpath = "$.value[*]"
    next_page_token_jsonpath = "$.['@odata.nextLink']"
    expand = None

    def get_environments_list(self):
        if self.envs_list:
            return self.envs_list
        headers = {}
        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})
        # Client credentials use the admin endpoint; refresh tokens use the
        # tenant environments endpoint.
        if self.config.get("refresh_token"):
            url = "https://api.businesscentral.dynamics.com/environments/v1.1"
        else:
            url = "https://api.businesscentral.dynamics.com/admin/v2.0/applications/BusinessCentral/environments"

        def fetch_environments():
            response = requests.get(url=url, headers=headers, timeout=self.timeout)
            self.validate_response(response)
            return response

        envs_list = self.request_decorator(fetch_environments)().json()
        self.envs_list = envs_list
        return self.envs_list

    def validate_env(self, env_name):
        env_name = env_name.lower()
        envs_list = self.get_environments_list()
        if "value" in envs_list:
            for env in envs_list["value"]:
                # Check for valid environment name is provided. Tenant ID is optional for requesting companies etc.
                if env["name"].lower() in env_name:
                    return True

        raise Exception("Invalid environment name provided.")

    @property
    def authenticator(self) -> TapDynamicsBCAuth:
        """Return a new authenticator object."""
        return TapDynamicsBCAuth.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {"If-Match": "*", "Prefer": f"odata.maxpagesize={self.page_size}"}

        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            next_page_link = next(iter(all_matches), None)
            if not next_page_link:
                return None

            # Parse the URL
            parsed_url = urlparse(str(next_page_link))
            # Extract the query parameters
            query_params = parse_qs(parsed_url.query)
            aid_value = query_params.get("aid")
            skiptoken_value = query_params.get("$skiptoken")
            # If $skiptoken exists, get its first value (as it can be a list)
            if aid_value and skiptoken_value:
                if isinstance(aid_value, list):
                    aid_value = aid_value[0]
                if isinstance(skiptoken_value, list):
                    skiptoken_value = skiptoken_value[0]
                return "&aid=" + aid_value + "&$skiptoken=" + skiptoken_value

        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                date = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                params["$filter"] = f"{self.replication_key} gt {date}"
        if self.expand:
            params["$expand"] = self.expand
        if next_page_token:
            params["aid"] = next_page_token.split("aid=")[-1].split("&")[0]
            params["$skiptoken"] = next_page_token.split("$skiptoken=")[-1]
        return params

    def make_request(self, context, next_page_token):
        prepared_request = self.prepare_request(
            context, next_page_token=next_page_token
        )
        resp = self._request(prepared_request, context)
        return resp

    def make_request_with_adaptive_page_size(
        self,
        context,
        next_page_token,
        *,
        minimum_page_size: int = 10,
    ):
        """Retry with smaller page sizes when a page exceeds the read timeout.

        Halves ``page_size`` on each ``ReadTimeout`` until the request succeeds
        or ``minimum_page_size`` is reached. The winning size is kept on the
        stream instance for subsequent pages in the same company partition.
        """
        page_size = self.page_size
        last_error: Optional[requests.exceptions.ReadTimeout] = None

        while page_size >= minimum_page_size:
            self.page_size = page_size
            try:
                prepared_request = self.prepare_request(
                    context, next_page_token=next_page_token
                )
                return self._request(prepared_request, context)
            except requests.exceptions.ReadTimeout as err:
                last_error = err
                next_page_size = max(minimum_page_size, page_size // 2)
                if next_page_size >= page_size:
                    break
                self.logger.warning(
                    "Read timeout fetching %s at page_size=%s; retrying with page_size=%s",
                    self.name,
                    page_size,
                    next_page_size,
                )
                page_size = next_page_size

        if last_error is not None:
            raise last_error
        raise ValueError("page_size must be at least minimum_page_size")

    def request_records(self, context: Optional[dict]):
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self.make_request)

        while not finished:
            resp = decorated_request(context, next_page_token)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

    @staticmethod
    def _is_retriable_conflict(response: requests.Response) -> bool:
        """Return whether a conflict represents a transient BC deadlock."""
        if response.status_code != 409:
            return False

        try:
            error = response.json().get("error", {})
        except (AttributeError, ValueError):
            return False

        if not isinstance(error, dict):
            return False

        message = error.get("message", "")
        return (
            error.get("code") == "Internal_ServerError"
            and isinstance(message, str)
            and "deadlock" in message.lower()
            and "retry" in message.lower()
        )

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code in [401]:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path} with response {response.text}"
            )
            raise RetriableAPIError(msg)
        elif response.status_code == 429:
            msg = (
                f"{response.status_code} Too Many Requests: "
                f"{response.reason} for path: {self.path} with response {response.text}"
            )
            raise RetriableAPIError(msg)
        elif response.status_code == 400 and "Please try again later." in response.text:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path} with response {response.text}"
            )
            raise RetriableAPIError(msg)
        elif self._is_retriable_conflict(response):
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path} with response {response.text}"
            )
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path} with response {response.text}"
            )
            raise FatalAPIError(msg)
        elif 500 <= response.status_code < 600 or response.status_code in [401]:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path} with response {response.text}"
            )
            raise RetriableAPIError(msg)

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            empty_streams = []
            for stream_name, stream_state in tap_state.get("bookmarks").items():
                if stream_name in [
                    "gl_entries_dimensions",
                ] and stream_state.get("partitions"):
                    tap_state["bookmarks"][stream_name] = {"partitions": []}
                    stream_state = tap_state["bookmarks"][stream_name]

                if stream_state.get("partitions"):
                    stream_state["partitions"] = [
                        partition
                        for partition in stream_state["partitions"]
                        if partition.get("replication_key_value")
                        or partition.get("replication_key")
                        or partition.get("progress_markers")
                        or partition.get("starting_replication_value")
                        or partition.get("replication_key_signpost")
                    ]

                if not stream_state or stream_state == {"partitions": []}:
                    empty_streams.append(stream_name)

            for stream_name in empty_streams:
                tap_state["bookmarks"].pop(stream_name, None)

        singer.write_message(StateMessage(value=tap_state))

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        context_values = context or {}
        for schema_field in self.schema.get("properties", {}):
            if schema_field in context_values and schema_field not in row:
                row[schema_field] = context_values[schema_field]

        return row


class DynamicsBCODataStream(dynamicsBcStream):
    """Dynamics BC OData stream class."""

    @cached_property
    def url_base(self):
        env_name = self.config.get("environment_name", "Production")
        if "?" in env_name:
            env_name = env_name.split("?")[0]
        environments = self.get_environments_list()["value"]
        chosen_environment = next(
            (
                env
                for env in environments
                if env["name"].lower() == env_name.lower()
                or env["name"].lower() in env_name.lower()
            ),
            None,
        )
        if not chosen_environment:
            raise Exception("No environment with name: " + env_name)
        return f"https://api.businesscentral.dynamics.com/v2.0/{chosen_environment['aadTenantId']}/{chosen_environment['name']}/ODataV4"

    def _is_initial_sync(self, context: Optional[dict]) -> bool:
        """Return whether no finalized replication-key bookmark exists yet."""
        state = self.get_context_state(context)
        return not state.get("replication_key_value")

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        # Header records appear with empty values and should be skipped.
        if all(value == "" for key, value in row.items() if key != "@odata.etag"):
            return None
        return super().post_process(row, context)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        # Include unmodified records on the initial sync; their sentinel value is
        # older than start_date and the normal greater-than filter excludes them.
        if self.replication_key and self._is_initial_sync(context):
            start_date = self.get_starting_timestamp(context)
            if start_date:
                date = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                params["$filter"] = (
                    f"({self.replication_key} gt {date}) or "
                    f"({self.replication_key} eq {BC_DEFAULT_MODIFIED_SENTINEL})"
                )
        return params


class OptiplyCustomExtensionBCDataStream(dynamicsBcStream):
    """Dynamics BC Optiply Custom Extension stream class."""

    @cached_property
    def url_base(self):
        env_name = self.config.get("environment_name", "Production")
        if "?" in env_name:
            env_name = env_name.split("?")[0]
        environments = self.get_environments_list()["value"]
        chosen_environment = next(
            (
                env
                for env in environments
                if env["name"].lower() == env_name.lower()
                or env["name"].lower() in env_name.lower()
            ),
            None,
        )
        if not chosen_environment:
            raise Exception("No environment with name: " + env_name)
        return f"https://api.businesscentral.dynamics.com/v2.0/{chosen_environment['aadTenantId']}/{chosen_environment['name']}/api/optiply/integration/v1.0"


class DynamicsBCAnalyticsStream(dynamicsBcStream):
    """Dynamics BC Analytics stream class."""

    page_size = 1000

    @cached_property
    def url_base(self):
        environment = self.get_environment()
        return f"https://api.businesscentral.dynamics.com/v2.0/{environment}/api/microsoft/analytics/v1.0"

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        records = response.json().get("value", [])
        if not records:
            return None
        previous_skip: int = previous_token or 0
        next_skip = previous_skip + len(records)
        if len(records) < self.page_size:
            return None
        return next_skip

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {"$top": self.page_size}
        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                date = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                params["$filter"] = f"{self.replication_key} gt {date}"
        if next_page_token:
            params["$skip"] = next_page_token
        return params
