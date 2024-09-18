"""REST client handling, including dynamics-bcStream base class."""

from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_dynamics_bc.auth import TapDynamicsBCAuth
from backports.cached_property import cached_property


class dynamicsBcStream(RESTStream):
    """dynamics-bc stream class."""
    envs_list = None

    @cached_property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        url_template = "https://api.businesscentral.dynamics.com/v2.0/{}/api/v2.0"
        env_name = self.config.get("environment_name", "production")
        if "?" in env_name:
            env_name = env_name.split("?")
            if isinstance(env_name,list):
                env_name = env_name[0]
        self.validate_env(env_name)        
        return url_template.format(env_name)

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
        envs_list = requests.get(url="https://api.businesscentral.dynamics.com/environments/v1.1",headers=headers)
        self.validate_response(envs_list)
        envs_list = envs_list.json()
        self.envs_list = envs_list
        return self.envs_list
        

    def validate_env(self,env_name):
        env_name = env_name.lower()
        envs_list = self.get_environments_list()
        if "value" in envs_list:
            for env in envs_list['value']:
                #Check for valid environment name is provided. Tenant ID is optional for requesting companies etc.
                if env['name'].lower() in env_name:
                    return True
                    
        raise Exception("Invalid environment name provided.")    
    @property
    def authenticator(self) -> TapDynamicsBCAuth:
        """Return a new authenticator object."""
        return TapDynamicsBCAuth.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
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
            first_match = next(iter(all_matches), None)
            next_page_link = first_match

            # Parse the URL
            parsed_url = urlparse(next_page_link)
            # Extract the query parameters
            query_params = parse_qs(parsed_url.query)
            aid_value = query_params.get('aid')
            skiptoken_value = query_params.get('$skiptoken')
            # If $skiptoken exists, get its first value (as it can be a list)
            if aid_value and skiptoken_value:
                if type(aid_value) == list:
                    aid_value = aid_value[0]
                if type(skiptoken_value) == list:
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
            date = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            params["$filter"] = f"{self.replication_key} gt {date}"
        if self.expand:
            params["$expand"] = self.expand
        if next_page_token:
            params["aid"] = next_page_token.split("aid=")[-1].split("&")[0]
            params["$skiptoken"] = next_page_token.split("$skiptoken=")[-1]
        return params