from django.urls import path

from . import views as connector_views

urlpatterns = [
    path('test_connection', connector_views.connectors_test_connection),
]
