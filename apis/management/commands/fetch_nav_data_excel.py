from django.core.management.base import BaseCommand
import requests
from datetime import datetime, timedelta
import logging
import io
import zipfile
import openpyxl
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from django.db import transaction

logger = logging.getLogger(__name__)

MAX_ROWS_PER_SHEET = 1_000_001


class Command(BaseCommand):
    help = 'Fetch NAV data for a date range and export to a zip file containing an Excel file'

    def add_arguments(self, parser):
        parser.add_argument('start_date', type=str, help='Start date in dd-MMM-yyyy format (e.g., 01-Apr-2006)')
        parser.add_argument('end_date', type=str, help='End date in dd-MMM-yyyy format (e.g., 31-Mar-2008)')
        parser.add_argument('--output-file', type=str, help='Output zip file path', default='D:/Ems/nav_data.zip')

    def handle(self, *args, **options):
        logger.info("Starting fetch_nav_data command")
        start_date = datetime.strptime(options['start_date'], '%d-%b-%Y').date()
        end_date = datetime.strptime(options['end_date'], '%d-%b-%Y').date()
        output_file = options['output_file']

        try:
            # Initialize zip file
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                current_date = start_date
                data = []
                total_records = 0

                while current_date <= end_date:
                    records_fetched = self.fetch_data_for_date(current_date, data)
                    if records_fetched is not None:
                        self.stdout.write(
                            self.style.SUCCESS(f'Successfully fetched {records_fetched} records for {current_date}'))
                        total_records += records_fetched
                    else:
                        self.stdout.write(self.style.ERROR(f'Failed to fetch data for {current_date}'))

                    current_date += timedelta(days=1)

                self.export_to_excel(data, zip_file)

            # Save zip file
            with open(output_file, 'wb') as f:
                f.write(zip_buffer.getvalue())

            self.stdout.write(self.style.SUCCESS(f'Successfully saved NAV data to {output_file}'))
            self.stdout.write(self.style.SUCCESS(f'Total records fetched: {total_records}'))

        except IOError as e:
            error_msg = f'Error writing to file {output_file}: {str(e)}'
            self.stdout.write(self.style.ERROR(error_msg))
            logger.error(error_msg, exc_info=True)

    def fetch_data_for_date(self, date, data):
        date_str = date.strftime('%d-%b-%Y')
        url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={date_str}"
        logger.info(f"Fetching data for date: {date_str}")

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))

        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()

            data_lines = response.text.splitlines()

            current_amc_name = None
            nav_count = 0

            for line in data_lines:
                try:
                    line = line.strip()
                    if not line or line.startswith("Open Ended Schemes") or line.startswith("Close Ended Schemes"):
                        continue

                    if ';' not in line:
                        current_amc_name = line.strip()
                        continue

                    if current_amc_name and ';' in line:
                        fields = line.split(';')
                        if len(fields) < 8:
                            continue

                        scheme_name = fields[1]
                        net_asset_value = fields[4]
                        nav_date = fields[7]

                        try:
                            parsed_date = datetime.strptime(nav_date, '%d-%b-%Y').date() if nav_date else None
                        except ValueError:
                            parsed_date = None

                        data.append(
                            [parsed_date.strftime('%d-%b-%Y') if parsed_date else '', current_amc_name, scheme_name,
                             net_asset_value])
                        nav_count += 1

                except Exception as e:
                    logger.error(f"Error processing line: {line}. Error: {str(e)}")

            self.stdout.write(self.style.SUCCESS(f"\nRecords fetched for {date_str}: {nav_count}"))
            return nav_count

        except requests.exceptions.RequestException as e:
            error_msg = f'Error fetching data for {date_str}: {str(e)}'
            self.stdout.write(self.style.ERROR(error_msg))
            logger.error(error_msg, exc_info=True)
        except Exception as e:
            error_msg = f'Unexpected error processing data for {date_str}: {str(e)}'
            self.stdout.write(self.style.ERROR(error_msg))
            logger.error(error_msg, exc_info=True)

        return None

    def export_to_excel(self, data, zip_file):
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.title = 'NAV Data'

        sheet.append(['Date', 'Fund Family', 'Scheme Name', 'Net Asset Value'])

        current_row = 2
        sheet_number = 0
        for row_data in data:
            if current_row > MAX_ROWS_PER_SHEET:
                sheet_number += 1
                sheet = workbook.create_sheet(f'NAV Data {sheet_number}')
                sheet.append(['Date', 'Fund Family', 'Scheme Name', 'Net Asset Value'])
                current_row = 2
            sheet.append(row_data)
            current_row += 1

        with zip_file.open('nav_data.xlsx', 'w') as xlsx_file:
            workbook.save(xlsx_file)
