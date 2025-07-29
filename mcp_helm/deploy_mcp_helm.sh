#!/bin/bash

# MCP Helm Deployment Script
# Deploys the DRD VPC Agent in MCP mode using Helm

set -e

# Configuration
NAMESPACE=${NAMESPACE:-"drdroid"}
RELEASE_NAME=${RELEASE_NAME:-"drd-vpc-agent-mcp"}
CHART_PATH="$(dirname "$0")"

echo "🚀 Deploying DRD VPC Agent in MCP Mode using Helm"
echo "📊 Namespace: $NAMESPACE"
echo "📦 Release Name: $RELEASE_NAME"
echo "📁 Chart Path: $CHART_PATH"
echo ""

# Check if helm is installed
if ! command -v helm &> /dev/null; then
    echo "❌ Error: Helm is not installed. Please install Helm and try again."
    exit 1
fi

# Check if kubectl is configured
if ! kubectl cluster-info &> /dev/null; then
    echo "❌ Error: kubectl is not configured or cluster is not accessible."
    exit 1
fi

# Create namespace if it doesn't exist
echo "🔍 Checking if namespace '$NAMESPACE' exists..."
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "📦 Creating namespace '$NAMESPACE'..."
    kubectl create namespace "$NAMESPACE"
else
    echo "✅ Namespace '$NAMESPACE' already exists"
fi

# Apply configmap for credentials
echo "🔍 Applying credentials configmap..."
if [ -f "$CHART_PATH/configmap.yaml" ]; then
    kubectl apply -f "$CHART_PATH/configmap.yaml" -n "$NAMESPACE"
    echo "✅ Configmap applied successfully"
else
    echo "⚠️  Warning: configmap.yaml not found. Make sure to configure your credentials."
    echo "   Please add your connector credentials to configmap.yaml before deploying."
    read -p "   Do you want to continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Deployment cancelled. Please configure credentials first."
        exit 1
    fi
fi

# Deploy using Helm
echo "🚀 Deploying MCP server using Helm..."
helm upgrade --install "$RELEASE_NAME" "$CHART_PATH" \
    --namespace "$NAMESPACE" \
    --create-namespace \
    --wait \
    --timeout=5m

# Check deployment status
echo ""
echo "🔍 Checking deployment status..."
kubectl rollout status deployment/"$RELEASE_NAME"-mcp-server -n "$NAMESPACE" --timeout=300s

# Get service information
echo ""
echo "📊 Service Information:"
kubectl get service -n "$NAMESPACE" -l "app.kubernetes.io/instance=$RELEASE_NAME"

# Get pod information
echo ""
echo "🐳 Pod Information:"
kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$RELEASE_NAME"

echo ""
echo "✅ MCP Mode deployment completed successfully!"
echo ""
echo "🌐 To access the MCP server:"
echo "   kubectl port-forward -n $NAMESPACE service/mcp-server-service 8000:8000"
echo ""
echo "🔍 To check logs:"
echo "   kubectl logs -n $NAMESPACE -l component=mcp-server -f"
echo ""
echo "🗑️  To uninstall:"
echo "   helm uninstall $RELEASE_NAME -n $NAMESPACE"
echo ""
echo "📚 MCP endpoints will be available at:"
echo "   http://localhost:8000/playbooks/mcp/" 