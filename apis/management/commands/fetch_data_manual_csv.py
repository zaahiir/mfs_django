from django.core.management.base import BaseCommand
import requests
from datetime import datetime
import csv
import os


class Command(BaseCommand):
    help = 'Fetch NAV data for a specific date and export to CSV'

    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Date for which to fetch NAV data in dd-MMM-yyyy format (e.g., 14-Aug-2024)',
        )
        parser.add_argument(
            '--output-file',
            type=str,
            help='Full path to the output CSV file',
        )

    def handle(self, *args, **options):
        date_str = options['date']
        output_file = options['output_file']

        if not date_str:
            self.stdout.write(self.style.ERROR('Please provide a date using --date option'))
            return

        if not output_file:
            self.stdout.write(self.style.ERROR('Please provide an output file path using --output-file option'))
            return

        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        os.makedirs(output_dir, exist_ok=True)

        url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={date_str}"

        try:
            response = requests.get(url)
            response.raise_for_status()

            data_lines = response.text.splitlines()

            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(['Date', 'Fund Family', 'Scheme Name', 'Net Asset Value'])

                current_amc_name = None
                for line in data_lines:
                    line = line.strip()
                    if not line:
                        continue

                    if line.startswith("Open Ended Schemes") or line.startswith("Close Ended Schemes"):
                        continue

                    if not line[0].isdigit() and ';' not in line:
                        current_amc_name = line
                        continue

                    if current_amc_name and ';' in line:
                        fields = line.split(';')
                        if len(fields) < 8:
                            continue

                        scheme_name = fields[1]
                        net_asset_value = fields[4]
                        date = fields[7]

                        try:
                            parsed_date = datetime.strptime(date, '%d-%b-%Y').strftime('%d-%b-%Y')
                        except ValueError:
                            self.stdout.write(self.style.WARNING(f"Invalid date format: {date}"))
                            parsed_date = date

                        csvwriter.writerow([parsed_date, current_amc_name, scheme_name, net_asset_value])

            self.stdout.write(self.style.SUCCESS(f'Successfully saved NAV data to {output_file}'))

        except requests.exceptions.RequestException as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
