from django.urls import path
from . import mcp_views

urlpatterns = [
    path('mcp', mcp_views.mcp_endpoint),
]
