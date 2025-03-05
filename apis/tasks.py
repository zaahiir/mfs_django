from __future__ import absolute_import, unicode_literals

import datetime
import logging
from celery import shared_task
from django.core.management import call_command
import pytz

logger = logging.getLogger(__name__)


@shared_task(name='apis.tasks.fetch_daily_nav')
def fetch_daily_nav():
    logger.debug("fetch_daily_nav task started")
    try:
        result = call_command('fetch_nav_data')
        logger.info(f"fetch_daily_nav task completed. Result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in fetch_daily_nav: {str(e)}", exc_info=True)
        raise
