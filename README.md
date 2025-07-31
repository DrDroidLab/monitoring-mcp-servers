# MCP Server for AI Automation on your Monitoring Data

## About

Spinning up this repo gives you an endpoint that you can connect to any MCP Client (Claude Desktop, Cursor, DrDroid, etc.) and connect AI to your respective tools with just this change in your mcp.json
   ```shell
   {
      "mcpServers": {
         "multi-tool-mcp-server": {
            "url": "your_url/playbooks/mcp"
         }
      }
   }
   ```

For sample usage, clone [Slack AI Bot](https://github.com/DrDroidLab/slack-ai-bot-builder) and create your own AI Slack Bot that interacts with the following sources:
- Grafana -- logs, metrics, dashboards fetching and analysis
- Kubernetes -- run kubectl commands
- Grafana Loki -- logs fetching (Can also be done via Grafana)
- Signoz -- Query your OpenTelemetry data directly from Signoz / Clickhouse.
- Bash Commands -- Run commands on terminal

Additional support: Apart from these, there is also support for 30+ other tools. Browse full list of integrations supported [here](https://github.com/DrDroidLab/drd-vpc-agent/tree/mcp_main/integrations/source_manangers)

## Installation for MCP Mode

**Note:**
* If you want the MCP Server to be connected to your k8s cluster without any additional configuration effort, install using the HELM chart.

### Docker Compose (MCP Mode)

1. Add your credentials in `credentials/secrets.yaml`
2. Run the MCP server:
   ```shell
   docker compose -f mcp.docker-compose.yaml up --build -d
   ```
3. Add it to your Cursor using the localhost setup with this `mcp.json` configuration:
   ```json
   {
     "mcpServers": {
       "droid-vpc-agent": {
         "url": "http://localhost:8000/playbooks/mcp"
       }
     }
   }
   ```

### Helm (MCP Mode)

For Helm deployment, the process is the same as the normal Helm deployment:

1. Add the secrets for the integrations in helm/configmap.yaml file.
   Refer to the image below for a sample:
   <img width="934" alt="Screenshot 2024-12-20 at 14 02 43" src="https://github.com/user-attachments/assets/cadb2b0a-db0c-4128-bef7-fe2a6288b79b" />

Command:

```shell
cd mcp_helm
./deploy_mcp_helm.sh
```

Note:
* The agent will be installed in the namespace 'drdroid' by default. This can be changed in the helm/deploy_helm.sh
  file.
* Agent will have read access to the cluster and will be able to fetch the metrics from the cluster.

### Local Development (MCP Mode)

1. Create and activate a virtual environment:
   ```shell
   uv venv env
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```shell
   uv pip sync requirements.txt
   ```
3. Start the MCP server:
   ```shell
   sh start_mcp_server.sh
   ```
[TODO] -- add instructions on how to port forward and test kubernetes from local.

## Installation -- VPC Mode

Apart from the MCP mode, we also have VPC mode. The integrations accessible are same, but in this mode:
* No ingress port is required to be open on the machine where this service is deployed. (This operates on a redis + polling model)
* This is designed for DrDroid Cloud users who want to connect our platform to integrations that are behind their VPC.
* Read instructions on installation & getting started on [main branch](https://github.com/DrDroidLab/drd-vpc-agent/tree/main/).

## Support

For any queries or help in setup, join [Discord](https://discord.gg/AQ3tusPtZn)

## Contributions

We welcome contributions. If you have any suggestions or improvements, please feel free to open an issue or submit a pull request. We appreciate your help in making this project better!
