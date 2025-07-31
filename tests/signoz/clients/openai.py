from tests.shared.clients.openai import OpenAIMCPClient as BaseOpenAIMCPClient
from tests.signoz.clients.signoz import SignozMCPClient


class OpenAIMCPClient(BaseOpenAIMCPClient):
    def __init__(
        self,
        test_client,
        openai_api_key,
        mcp_api_key: str = "test-key",
    ):
        """
        Initialize the OpenAI-MCP Client for Signoz testing.
        """
        super().__init__(
            test_client=test_client,
            openai_api_key=openai_api_key,
            mcp_client_class=SignozMCPClient,
            mcp_api_key=mcp_api_key,
        ) 