# DRD VPC Agent - MCP Mode Helm Chart

This Helm chart deploys the DRD VPC Agent in MCP (Model Context Protocol) mode on Kubernetes. This is a lightweight deployment that includes only the Django server for MCP functionality, without Redis, Celery workers, or other background services.

## Features

- ✅ **Django MCP Server**: Only the necessary Django server for MCP endpoints
- ✅ **Direct Metadata Access**: Uses metadata extractors directly (no database dependencies)
- ✅ **Kubernetes Native**: Designed for Kubernetes deployments
- ✅ **RBAC Support**: Includes necessary service accounts and cluster roles
- ✅ **Health Checks**: Built-in liveness and readiness probes
- ✅ **Credentials Management**: ConfigMap-based credential management
- ❌ **No DRD Cloud Token Required**: Completely standalone
- ❌ **No Redis/Celery**: Minimal resource footprint

## Prerequisites

- Kubernetes cluster (v1.19+)
- Helm 3.x
- kubectl configured to access your cluster

## Quick Start

### 1. Configure Credentials

Edit the `configmap.yaml` file to include your connector credentials:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: credentials-config
data:
  secrets.yaml: |
    grafana:
      - name: "my-grafana"
        grafana_host: "https://your-grafana.com"
        grafana_api_key: "your-api-key"
        ssl_verify: "true"
```

### 2. Deploy using the script

```bash
cd mcp_helm
./deploy_mcp_helm.sh
```

### 3. Or deploy manually with Helm

```bash
# Create namespace
kubectl create namespace drdroid

# Deploy the chart
helm install drd-vpc-agent-mcp ./mcp_helm --namespace drdroid

# Check status
kubectl get pods -n drdroid
```

## Configuration

### Values.yaml Configuration

The chart can be customized through the `values.yaml` file:

```yaml
# MCP Server Configuration
mcpServer:
  image:
    repository: public.ecr.aws/y9s1f3r5/drdroid/drd-vpc-agent
    tag: latest
    pullPolicy: Always
  
  service:
    type: ClusterIP
    port: 8000
    targetPort: 8000
  
  replicas: 1
  
  resources:
    requests:
      memory: "256Mi"
      cpu: "250m"
    limits:
      memory: "512Mi"
      cpu: "500m"
```

### Environment Variables

The following environment variables are automatically configured:

- `DJANGO_DEBUG`: Set to "True" for development
- `NATIVE_KUBERNETES_API_MODE`: Set to "true" 
- `CELERY_BROKER_URL`: Empty (no Celery)
- `CELERY_RESULT_BACKEND`: Empty (no Celery)
- `REDIS_URL`: Empty (no Redis)

## Accessing the MCP Server

### Port Forward (Development)

```bash
kubectl port-forward -n drdroid service/mcp-server-service 8000:8000
```

The MCP server will be accessible at `http://localhost:8000`

### Service Types

#### ClusterIP (Default)
- Internal cluster access only
- Use port-forward for external access

#### NodePort
```yaml
mcpServer:
  service:
    type: NodePort
    port: 8000
    nodePort: 30800  # Optional
```

#### LoadBalancer
```yaml
mcpServer:
  service:
    type: LoadBalancer
    port: 8000
```

## Available Endpoints

Once deployed, the following MCP endpoints will be available:

### Grafana MCP Tools
- `POST /mcp/grafana/query_dashboard_panel_metric`
- `POST /mcp/grafana/execute_all_dashboard_panels` 
- `POST /mcp/grafana/fetch_dashboard_variables`

### Health Check
- `GET /health`

## Monitoring and Troubleshooting

### Check Pod Status
```bash
kubectl get pods -n drdroid -l component=mcp-server
```

### View Logs
```bash
kubectl logs -n drdroid -l component=mcp-server -f
```

### Debug Deployment
```bash
kubectl describe deployment -n drdroid
kubectl describe pod -n drdroid -l component=mcp-server
```

### Health Checks

The deployment includes:
- **Liveness Probe**: Checks `/health` endpoint every 30 seconds
- **Readiness Probe**: Checks `/health` endpoint every 10 seconds

## Scaling

### Manual Scaling
```bash
kubectl scale deployment -n drdroid deployment-name --replicas=3
```

### Update Values
```yaml
mcpServer:
  replicas: 3
```

Then upgrade:
```bash
helm upgrade drd-vpc-agent-mcp ./mcp_helm --namespace drdroid
```

## Security

### RBAC
The chart includes:
- ServiceAccount: `drd-vpc-agent`
- ClusterRole: `drdroid-k8s-cluster-role` (read-only access to cluster resources)
- ClusterRoleBinding: Links the service account to the cluster role

### Pod Security
```yaml
podSecurityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
```

## Upgrading

```bash
helm upgrade drd-vpc-agent-mcp ./mcp_helm --namespace drdroid
```

## Uninstalling

```bash
helm uninstall drd-vpc-agent-mcp --namespace drdroid
```

## Customization Examples

### Custom Image
```yaml
mcpServer:
  image:
    repository: your-registry/drd-agent
    tag: custom-tag
```

### Resource Limits
```yaml
mcpServer:
  resources:
    requests:
      memory: "512Mi"
      cpu: "500m"
    limits:
      memory: "1Gi"
      cpu: "1"
```

### Different Namespace
```bash
helm install drd-vpc-agent-mcp ./mcp_helm --namespace my-namespace --create-namespace
```

## Troubleshooting

### Common Issues

1. **Pod not starting**
   - Check credentials configuration
   - Verify image pull permissions
   - Check resource availability

2. **Health check failures**
   - Check if Django server is starting properly
   - Verify port 8000 is accessible
   - Check logs for startup errors

3. **Permission issues**
   - Verify RBAC resources are created
   - Check service account permissions

### Getting Help

1. Check pod logs: `kubectl logs -n drdroid -l component=mcp-server`
2. Describe resources: `kubectl describe pod -n drdroid -l component=mcp-server`
3. Check events: `kubectl get events -n drdroid --sort-by='.lastTimestamp'` 