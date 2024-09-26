from __future__ import absolute_import, unicode_literals

import logging
import os

from celery import Celery, shared_task

logger = logging.getLogger(__name__)

# Set the default Django settings mode for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'agent.settings')

app = Celery('agent')
app.config_from_object('django.conf:settings', namespace='CELERY')

app.autodiscover_tasks()


@shared_task
def debug_task():
    logger.info('Debug task executed')
