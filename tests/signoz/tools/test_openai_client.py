import pytest

from tests.signoz.conftest import assert_response_quality

# Mark all tests in this file as 'integration'
pytestmark = pytest.mark.integration

# Test data for parameterized testing
test_models = ["gpt-4.1-2025-04-14"]
service_queries = [
    "Fetch all services from SigNoz.",
    "Show me the available services.", 
    "List all services in SigNoz.",
]
connection_queries = [
    "Test the connection to SigNoz.",
    "Check SigNoz connection status.",
    "Verify connection to SigNoz.",
]
dashboard_queries = [
    "Fetch all available dashboards from SigNoz.",
    "Fetch all data from the Python Microservices dashboard.",
    "Fetch all data from the Go Microservices dashboard.",
]
traces_queries = [
    "Show me all recent traces from SigNoz.",
    "Get traces for the recommendationservice from signoz",
]
logs_queries = [
    "Show me recent logs from SigNoz.",
    "Get log entries for the last hour.",
    "Fetch logs for error-level messages.",
]


class TestOpenAIIntegration:
    """Test OpenAI integration with SigNoz MCP server."""

    @pytest.mark.parametrize("model", test_models)
    @pytest.mark.parametrize("query", connection_queries)
    @pytest.mark.flaky(max_runs=3)
    @pytest.mark.pass_rate(0.8)
    def test_connection_queries(self, mcp_client, evaluator, model, query):
        """Test connection-related queries to SigNoz."""
        messages = [{"role": "user", "content": query}]
        response = mcp_client.chat(messages, model=model)
        print(f"Response: {response}")
        assert response, "Response should not be empty"
        assert_response_quality(
            prompt=query,
            response=response,
            evaluator=evaluator,
            specific_checks=["connection_status"],
            required_checks=["is_helpful"]
        )

    @pytest.mark.parametrize("model", test_models)
    @pytest.mark.parametrize("query", service_queries)
    @pytest.mark.flaky(max_runs=3)
    @pytest.mark.pass_rate(0.8)
    def test_service_queries(self, mcp_client, evaluator, model, query):
        """Test service-related queries to SigNoz."""
        messages = [{"role": "user", "content": query}]
        response = mcp_client.chat(messages, model=model)
        print(f"Response: {response}")
        
        assert response, "Response should not be empty"
        assert_response_quality(
            prompt=query,
            response=response,
            evaluator=evaluator,
            specific_checks=["services_info"],
            required_checks=["is_helpful"]
        )

    @pytest.mark.parametrize("model", test_models)
    @pytest.mark.parametrize("query", dashboard_queries)
    @pytest.mark.flaky(max_runs=3)
    @pytest.mark.pass_rate(0.8)
    def test_dashboard_queries(self, mcp_client, evaluator, model, query):
        """Test dashboard-related queries to SigNoz."""
        messages = [{"role": "user", "content": query}]
        response = mcp_client.chat(messages, model=model)
        
        assert response, "Response should not be empty"
        assert_response_quality(
            prompt=query,
            response=response,
            evaluator=evaluator,
            specific_checks=["dashboard_info"],
            required_checks=["is_helpful"]
        )

    @pytest.mark.parametrize("model", test_models)
    @pytest.mark.parametrize("query", traces_queries)
    @pytest.mark.flaky(max_runs=3)
    @pytest.mark.pass_rate(0.8)
    def test_traces_queries(self, mcp_client, evaluator, model, query):
        """Test trace-related queries to SigNoz."""
        messages = [{"role": "user", "content": query}]
        response = mcp_client.chat(messages, model=model)
        
        assert response, "Response should not be empty"
        assert_response_quality(
            prompt=query,
            response=response,
            evaluator=evaluator,
            specific_checks=["traces_info"],
            required_checks=["is_helpful"]
        )

    @pytest.mark.parametrize("model", test_models)
    @pytest.mark.parametrize("query", logs_queries)
    @pytest.mark.flaky(max_runs=3)
    @pytest.mark.pass_rate(0.8)
    def test_logs_queries(self, mcp_client, evaluator, model, query):
        """Test log-related queries to SigNoz."""
        messages = [{"role": "user", "content": query}]
        response = mcp_client.chat(messages, model=model)
        
        assert response, "Response should not be empty"
        assert_response_quality(
            prompt=query,
            response=response,
            evaluator=evaluator,
            specific_checks=["logs_info"],
            required_checks=["is_helpful"]
        )

