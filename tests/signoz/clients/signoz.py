import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class SignozMCPClient:
    """
    Client for interacting with the Signoz MCP server for testing purposes.
    Wraps the Django test client and provides MCP protocol methods.
    """

    def __init__(self, test_client: Any, api_key: str = "test-key"):
        """
        Initialize the Signoz MCP client.

        Args:
            test_client: Django test client instance
            api_key: API key for MCP server (for testing)
        """
        self.test_client = test_client
        self.api_key = api_key
        self.session_initialized = False
        self.mcp_url = "/playbooks/mcp"
        self._initialize_session()

    def _initialize_session(self):
        """Initialize the MCP session."""
        try:
            response = self.test_client.post(
                self.mcp_url,
                data=json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-06-18", 
                            "capabilities": {}, 
                            "clientInfo": {
                                "name": "test-client", 
                                "version": "1.0.0"
                            }
                        },
                        "id": "init-1",
                    }
                ),
                content_type="application/json",
            )

            if response.status_code == 200:
                self.session_initialized = True
                logger.info("MCP session initialized successfully")
            else:
                logger.error(f"Failed to initialize MCP session: {response.status_code}")

        except Exception as e:
            logger.error(f"Error initializing MCP session: {e}")

    def list_tools(self) -> List[Dict[str, Any]]:
        """List available tools from the MCP server."""
        if not self.session_initialized:
            logger.warning("Session not initialized")
            return []

        try:
            response = self.test_client.post(
                self.mcp_url,
                data=json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "tools/list",
                        "params": {},
                        "id": "tools-list-1",
                    }
                ),
                content_type="application/json",
            )

            if response.status_code == 200:
                result = response.json()
                if "result" in result:
                    return result["result"].get("tools", [])
                else:
                    logger.error(f"Error in tools/list response: {result}")
                    return []
            else:
                logger.error(f"Tools list request failed: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Error listing tools: {e}")
            return []

    def execute_tool(
        self, tool_name: str, parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a tool with given parameters."""
        if not self.session_initialized:
            logger.warning("Session not initialized")
            return {"error": "Session not initialized"}

        try:
            response = self.test_client.post(
                self.mcp_url,
                data=json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "tools/call",
                        "params": {
                            "name": tool_name,
                            "arguments": parameters,
                        },
                        "id": f"tool-call-{tool_name}-1",
                    }
                ),
                content_type="application/json",
            )

            if response.status_code == 200:
                result = response.json()
                if "result" in result:
                    return result["result"]
                else:
                    logger.error(f"Error in tool execution response: {result}")
                    return {"error": f"Tool execution failed: {result}"}
            else:
                logger.error(f"Tool execution request failed: {response.status_code}")
                return {"error": f"HTTP {response.status_code}"}

        except Exception as e:
            logger.error(f"Error executing tool {tool_name}: {e}")
            return {"error": str(e)}

    def close_session(self):
        """Close the MCP session (placeholder for cleanup)."""
        self.session_initialized = False
        logger.info("MCP session closed") 