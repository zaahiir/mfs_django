import json
import requests
from django.core.management.base import BaseCommand
from django.db import transaction
from apis.models import CountryModel  # Replace 'your_app' with your actual app name


class Command(BaseCommand):
    help = 'Populate the CountryModel with data from a GitHub gist'

    def handle(self, *args, **options):
        url = "https://gist.githubusercontent.com/anubhavshrimal/75f6183458db8c453306f93521e93d37/raw/f77e7598a8503f1f70528ae1cbf9f66755698a16/CountryCodes.json"

        try:
            response = requests.get(url)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            countries = response.json()

            with transaction.atomic():
                for country in countries:
                    CountryModel.objects.update_or_create(
                        countryCode=country['code'],
                        defaults={
                            'countryName': country['name'],
                            'dailCode': country['dial_code'],
                        }
                    )

            self.stdout.write(self.style.SUCCESS('Successfully populated CountryModel'))
        except requests.RequestException as e:
            self.stdout.write(self.style.ERROR(f'Failed to fetch data: {e}'))
        except json.JSONDecodeError:
            self.stdout.write(self.style.ERROR('Failed to parse JSON data'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'An error occurred: {e}'))
