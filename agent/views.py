import logging

from django.conf import settings
from django.http import HttpRequest, JsonResponse

from utils.decorators import account_get_api

logger = logging.getLogger(__name__)

loaded_connections = settings.LOADED_CONNECTIONS


@account_get_api()
def status(request_message: HttpRequest) -> JsonResponse:
    return JsonResponse({'status': 'ok'})
