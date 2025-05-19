from typing import Dict, Any
import requests
from requests.exceptions import RequestException
from agent.settings import DRD_CLOUD_API_TOKEN

class PrototypeClient:
    """
    Client for interacting with the DrDroid Platform.
    
    This client provides methods to interact with various DrDroid Platform APIs
    in a clean and type-safe manner.
    """

    def __init__(self, auth_token: str):
        """
        Initialize the client.
        
        Args:
            auth_token (str): Bearer token for authentication
        """
        if not auth_token:
            raise ValueError("auth_token is required")

        self.auth_token = auth_token
        self.base_url = DRD_CLOUD_API_TOKEN

    def _get_headers(self) -> Dict[str, str]:
        """Get the default headers for API requests."""
        return {
            'content-type': 'application/json',
            'Authorization': f'Bearer {self.auth_token}',
        }

    def get_connector_assets(
        self,
        connector_type: str,
        connector_id: str,
    ) -> Dict[str, Any]:
        """
        Retrieve connector assets based on specified parameters.

        Args:
            connector_type (str): Type of the connector (e.g., 'CLOUDWATCH')
            connector_id (str): ID of the connector

        Returns:
            Dict[str, Any]: Response data from the API

        Raises:
            Exception: If the API request fails
        """
        payload = {
            "connector_type": connector_type,
            "connector_id": connector_id,
        }

        try:
            response = requests.post(
                f"{self.base_url}/connectors/proxy/assets/models/get",
                json=payload,
                headers=self._get_headers()
            )
            response.raise_for_status()
            return response.json()

        except RequestException as e:
            raise Exception(f"Failed to get connector assets: {str(e)}") from e
        except Exception as e:
            raise Exception(f"Failed to get connector assets: {str(e)}") from e
