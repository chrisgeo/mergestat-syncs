"""
REST API helper utilities.

Provides common functionality for working with REST APIs,
particularly for GitLab's REST API.
"""

import logging
from typing import Any, Dict, List, Optional

import requests

from connectors.exceptions import (APIException, AuthenticationException,
                                   RateLimitException)
from connectors.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class RESTClient:
    """
    Generic REST API client with retry and rate limit handling.
    """

    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        timeout: int = 30,
        headers: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize REST client.

        :param base_url: Base URL for the API.
        :param token: Optional authentication token.
        :param timeout: Request timeout in seconds.
        :param headers: Optional additional headers.
        """
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.headers = headers or {}

        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    @retry_with_backoff(
        max_retries=5,
        initial_delay=1.0,
        max_delay=60.0,
        exceptions=(RateLimitException, APIException),
    )
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Make a GET request to the API.

        :param endpoint: API endpoint (relative to base_url).
        :param params: Optional query parameters.
        :param headers: Optional additional headers.
        :return: Response data.
        :raises AuthenticationException: If authentication fails.
        :raises RateLimitException: If rate limit is exceeded.
        :raises APIException: If API returns an error.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        request_headers = {**self.headers, **(headers or {})}

        try:
            response = requests.get(
                url,
                params=params,
                headers=request_headers,
                timeout=self.timeout,
            )

            # Check for HTTP errors
            if response.status_code == 401:
                raise AuthenticationException("Authentication failed")
            elif response.status_code == 403:
                raise APIException(f"Forbidden: {response.text}")
            elif response.status_code == 429:
                raise RateLimitException("API rate limit exceeded")
            elif response.status_code == 404:
                raise APIException(f"Not found: {endpoint}")
            elif response.status_code != 200:
                raise APIException(
                    f"API error: {response.status_code} - {response.text}"
                )

            return response.json()

        except requests.exceptions.Timeout:
            raise APIException("Request timeout")
        except requests.exceptions.RequestException as e:
            raise APIException(f"Request failed: {e}")

    @retry_with_backoff(
        max_retries=5,
        initial_delay=1.0,
        max_delay=60.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_list(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Make a GET request expecting a list response.

        :param endpoint: API endpoint (relative to base_url).
        :param params: Optional query parameters.
        :param headers: Optional additional headers.
        :return: List of response data.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        request_headers = {**self.headers, **(headers or {})}

        try:
            response = requests.get(
                url,
                params=params,
                headers=request_headers,
                timeout=self.timeout,
            )

            # Check for HTTP errors
            if response.status_code == 401:
                raise AuthenticationException("Authentication failed")
            elif response.status_code == 403:
                raise APIException(f"Forbidden: {response.text}")
            elif response.status_code == 429:
                raise RateLimitException("API rate limit exceeded")
            elif response.status_code == 404:
                raise APIException(f"Not found: {endpoint}")
            elif response.status_code != 200:
                raise APIException(
                    f"API error: {response.status_code} - {response.text}"
                )

            data = response.json()

            # Ensure we return a list
            if not isinstance(data, list):
                logger.warning(f"Expected list response, got {type(data)}")
                return []

            return data

        except requests.exceptions.Timeout:
            raise APIException("Request timeout")
        except requests.exceptions.RequestException as e:
            raise APIException(f"Request failed: {e}")


class GitLabRESTClient(RESTClient):
    """
    Specialized REST client for GitLab API.
    """

    def __init__(
        self,
        base_url: str = "https://gitlab.com/api/v4",
        private_token: Optional[str] = None,
        timeout: int = 30,
    ):
        """
        Initialize GitLab REST client.

        :param base_url: GitLab API base URL.
        :param private_token: GitLab private token.
        :param timeout: Request timeout in seconds.
        """
        headers = {}
        if private_token:
            headers["PRIVATE-TOKEN"] = private_token

        # Don't pass token to parent as we're using custom header
        super().__init__(
            base_url=base_url, token=None, timeout=timeout, headers=headers
        )

    def get_file_blame(
        self,
        project_id: int,
        file_path: str,
        ref: str = "main",
    ) -> List[Dict[str, Any]]:
        """
        Get blame information for a file in GitLab.

        :param project_id: GitLab project ID.
        :param file_path: Path to the file.
        :param ref: Git reference (branch, tag, or commit SHA).
        :return: List of blame ranges.
        """
        # URL-encode the file path
        import urllib.parse

        encoded_path = urllib.parse.quote(file_path, safe="")

        endpoint = f"projects/{project_id}/repository/files/{encoded_path}/blame"
        params = {"ref": ref}

        logger.debug(
            f"Fetching blame for project {project_id}, file {file_path} at ref {ref}"
        )
        return self.get_list(endpoint, params=params)

    def get_merge_requests(
        self,
        project_id: int,
        state: str = "all",
        page: int = 1,
        per_page: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get merge requests for a project.

        :param project_id: GitLab project ID.
        :param state: State filter ('opened', 'closed', 'merged', 'all').
        :param page: Page number.
        :param per_page: Results per page.
        :return: List of merge requests.
        """
        endpoint = f"projects/{project_id}/merge_requests"
        params = {
            "state": state,
            "page": page,
            "per_page": per_page,
        }

        logger.debug(f"Fetching merge requests for project {project_id}, page {page}")
        return self.get_list(endpoint, params=params)
