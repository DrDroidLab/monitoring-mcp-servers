from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
import os


@csrf_exempt
@require_http_methods(["GET"])
def health_check(request):
    """
    Simple health check endpoint for Kubernetes probes.
    Returns basic service status and configuration info.
    """
    return JsonResponse({
        "status": "healthy",
        "service": "drd-vpc-agent",
        "mode": os.getenv("DRD_AGENT_MODE", "unknown"),
        "version": "1.0.0"
    })
