from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from celery.schedules import crontab

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ems.settings')

app = Celery('ems')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# Set the timezone for Celery
app.conf.timezone = 'Asia/Kolkata'

# Celery Beat schedule configuration
app.conf.beat_schedule = {
    'fetch-daily-nav': {
        'task': 'apis.tasks.fetch_daily_nav',
        'schedule': crontab(hour=10, minute=30),  # This will use Asia/Kolkata timezone
    },
}


@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
