import requests
import csv
from django.core.management.base import BaseCommand
from io import StringIO


class Command(BaseCommand):
    help = 'Fetch and parse NAV data from AMFI portal to extract all keys'

    def handle(self, *args, **kwargs):
        # Define the URL
        url = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Aug-2024'

        try:
            # Send a GET request to the URL
            response = requests.get(url)
            response.raise_for_status()  # Raises an HTTPError for bad responses

            # Check if the content is text (since it looks like CSV data)
            content = response.content.decode('utf-8')

            # Use StringIO to read the content into a CSV reader
            csv_reader = csv.reader(StringIO(content), delimiter=';')

            # Extract the first row, which usually contains the keys/column names
            keys = next(csv_reader)

            # Print or return the keys (column headers)
            self.stdout.write(self.style.SUCCESS('Keys (Columns) from NAV data:'))
            for key in keys:
                self.stdout.write(key)

        except requests.exceptions.RequestException as e:
            self.stderr.write(self.style.ERROR(f'Error fetching data: {e}'))
