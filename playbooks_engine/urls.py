from django.urls import path

from . import views as playbooks_engine_views

urlpatterns = [
    # Playbook Run APIs
    path('task/run', playbooks_engine_views.task_run),
]
