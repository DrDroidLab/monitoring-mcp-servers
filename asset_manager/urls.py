from django.urls import path

from . import views as asset_views

urlpatterns = [
    path('models/fetch', asset_views.assets_models_fetch),
]
