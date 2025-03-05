from django.core.management.base import BaseCommand
from django.db import transaction, connection
from django.db.utils import IntegrityError
from apis.models import NavModel, AmcEntryModel, FundModel
from django.db.transaction import TransactionManagementError
import requests
from datetime import datetime, timedelta
import pytz
import logging
from collections import defaultdict
import time
from django.conf import settings
from psycopg2 import OperationalError

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Fetch and create new NAV data for a specific date, yesterday, or a date range'

    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Date for which to fetch NAV data in dd-MMM-yyyy format (e.g., 14-Aug-2024). If not provided, uses yesterday\'s date.',
            required=False,
        )
        parser.add_argument(
            '--start_date',
            type=str,
            help='Start date for fetching NAV data in dd-MMM-yyyy format (e.g., 14-Aug-2024)',
            required=False,
        )
        parser.add_argument(
            '--end_date',
            type=str,
            help='End date for fetching NAV data in dd-MMM-yyyy format (e.g., 20-Aug-2024)',
            required=False,
        )
        parser.add_argument(
            '--batch_size',
            type=int,
            default=50000,
            help='Batch size for database operations',
        )

    def handle(self, *args, **options):
        logger.info(f"Starting fetch_nav_data command at {datetime.now()}")
        try:
            date = options.get('date')
            start_date = options.get('start_date')
            end_date = options.get('end_date')
            self.batch_size = options.get('batch_size')

            self.records_per_day = defaultdict(int)
            self.records_per_month = defaultdict(int)
            self.total_records_fetched = 0
            self.total_records_processed = 0

            if start_date and end_date:
                self.fetch_date_range(start_date, end_date)
            elif date:
                self.fetch_single_date(date)
            else:
                self.fetch_yesterday()

            self.print_summary()

        except Exception as e:
            error_msg = f'Unexpected error: {str(e)}'
            self.stdout.write(self.style.ERROR(error_msg))
            logger.error(error_msg, exc_info=True)

    def fetch_date_range(self, start_date_str, end_date_str):
        start_date = datetime.strptime(start_date_str, '%d-%b-%Y')
        end_date = datetime.strptime(end_date_str, '%d-%b-%Y')

        current_date = start_date
        while current_date <= end_date:
            records = self.fetch_data_for_date(current_date)
            if records is None:
                self.stdout.write(
                    self.style.WARNING(f"Failed to fetch data for {current_date.date()}. Continuing to next date."))
            current_date += timedelta(days=1)

    def fetch_single_date(self, date_str):
        date = datetime.strptime(date_str, '%d-%b-%Y')
        self.fetch_data_for_date(date)

    def fetch_yesterday(self):
        kolkata_tz = pytz.timezone('Asia/Kolkata')
        yesterday = datetime.now(kolkata_tz) - timedelta(days=1)
        self.fetch_data_for_date(yesterday)

    def fetch_data_for_date(self, date):
        date_str = date.strftime('%d-%b-%Y')
        url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={date_str}"
        logger.info(f"Fetching data for date: {date_str}")

        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                data_lines = response.text.splitlines()
                nav_count = self.process_nav_data(data_lines, date)

                self.update_statistics(date, nav_count)
                self.stdout.write(self.style.SUCCESS(f"\nRecords fetched for {date_str}: {nav_count}"))
                return nav_count

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    self.stdout.write(
                        self.style.WARNING(f"Error fetching data for {date_str}. Retrying in {retry_delay} seconds..."))
                    time.sleep(retry_delay)
                else:
                    error_msg = f'Error fetching data for {date_str} after {max_retries} attempts: {str(e)}'
                    self.stdout.write(self.style.ERROR(error_msg))
                    logger.error(error_msg)
                    return None

            except Exception as e:
                error_msg = f'Error processing data for {date_str}: {str(e)}'
                self.stdout.write(self.style.ERROR(error_msg))
                logger.error(error_msg, exc_info=True)
                return None

    def process_nav_data(self, data_lines, date):
        nav_count = 0
        nav_data = []
        current_amc_name = None
        amc_cache = {}
        fund_cache = {}

        for line in data_lines:
            if not line.strip() or line.startswith("Open Ended Schemes") or line.startswith("Close Ended Schemes"):
                continue

            if ';' not in line:
                current_amc_name = line.strip()
                continue

            if current_amc_name and ';' in line:
                fields = line.split(';')
                if len(fields) < 8:
                    continue

                scheme_code, scheme_name, net_asset_value, nav_date = fields[0], fields[1], fields[4], fields[7]

                try:
                    amc_entry = self.get_or_create_amc(current_amc_name, amc_cache)
                    fund_entry = self.get_or_create_fund(amc_entry, scheme_name, scheme_code, fund_cache)

                    try:
                        parsed_date = datetime.strptime(nav_date, '%d-%b-%Y').date() if nav_date else None
                    except ValueError:
                        parsed_date = None

                    nav_data.append({
                        'navFundName': fund_entry,
                        'navDate': parsed_date,
                        'nav': net_asset_value
                    })

                    nav_count += 1

                    if len(nav_data) >= self.batch_size:
                        self.bulk_update_or_create_nav(nav_data)
                        nav_data = []

                except Exception as e:
                    logger.error(f"Error processing line: {line}. Error: {str(e)}")
                    continue

        if nav_data:
            self.bulk_update_or_create_nav(nav_data)

        return nav_count

    @transaction.atomic
    def get_or_create_amc(self, amc_name, amc_cache):
        if amc_name not in amc_cache:
            amc, created = AmcEntryModel.objects.get_or_create(amcName=amc_name)
            amc_cache[amc_name] = amc
        return amc_cache[amc_name]

    def get_or_create_fund(self, amc_entry, fund_name, scheme_code, fund_cache):
        key = (amc_entry.id, fund_name)
        if key not in fund_cache:
            try:
                fund = FundModel.objects.get(
                    fundAmcName=amc_entry,
                    fundName=fund_name
                )
                if fund.schemeCode != scheme_code and scheme_code and scheme_code != '-':
                    existing_fund = FundModel.objects.filter(schemeCode=scheme_code).first()
                    if existing_fund:
                        if existing_fund.fundName != fund_name:
                            logger.warning(
                                f"SchemeCode {scheme_code} conflict: '{fund_name}' vs '{existing_fund.fundName}'. Using existing fund.")
                            fund = existing_fund
                        else:
                            logger.info(f"Updating schemeCode for '{fund_name}' to {scheme_code}")
                            fund.schemeCode = scheme_code
                            fund.save()
                    else:
                        fund.schemeCode = scheme_code
                        fund.save()
            except FundModel.DoesNotExist:
                existing_fund = FundModel.objects.filter(schemeCode=scheme_code).first()
                if existing_fund:
                    logger.warning(
                        f"SchemeCode {scheme_code} already exists for '{existing_fund.fundName}'. Using existing fund.")
                    fund = existing_fund
                else:
                    fund = FundModel.objects.create(
                        fundAmcName=amc_entry,
                        fundName=fund_name,
                        schemeCode=scheme_code if scheme_code and scheme_code != '-' else None
                    )
            fund_cache[key] = fund
        return fund_cache[key]

    @transaction.atomic
    def bulk_update_or_create_nav(self, nav_data):
        existing_navs = NavModel.objects.filter(
            navFundName__in=[data['navFundName'] for data in nav_data],
            navDate__in=[data['navDate'] for data in nav_data]
        ).values('id', 'navFundName', 'navDate')

        existing_navs_dict = {
            (nav['navFundName'], nav['navDate']): nav['id']
            for nav in existing_navs
        }

        navs_to_update = []
        navs_to_create = []

        for nav in nav_data:
            key = (nav['navFundName'].id, nav['navDate'])
            if key in existing_navs_dict:
                nav['id'] = existing_navs_dict[key]
                navs_to_update.append(NavModel(**nav))
            else:
                navs_to_create.append(NavModel(**nav))

        try:
            NavModel.objects.bulk_create(navs_to_create, ignore_conflicts=True)
            NavModel.objects.bulk_update(navs_to_update, ['nav'])
        except IntegrityError as e:
            logger.error(f"Integrity error during bulk NAV operation: {str(e)}")
            self.handle_integrity_error(nav_data)

        self.total_records_processed += len(nav_data)

    def handle_integrity_error(self, nav_data):
        for nav in nav_data:
            try:
                NavModel.objects.update_or_create(
                    navFundName=nav['navFundName'],
                    navDate=nav['navDate'],
                    defaults={'nav': nav['nav']}
                )
            except IntegrityError as e:
                error_msg = f'Integrity error for NAV record: {nav}. Error: {str(e)}'
                logger.error(error_msg)
            except Exception as e:
                error_msg = f'Error processing NAV record: {nav}. Error: {str(e)}'
                logger.error(error_msg)

    def update_statistics(self, date, nav_count):
        self.records_per_day[date.date()] += nav_count
        self.records_per_month[(date.year, date.month)] += nav_count
        self.total_records_fetched += nav_count

    def print_summary(self):
        self.stdout.write(self.style.SUCCESS("\nSummary:"))
        self.stdout.write("Records fetched per day:")
        for date, count in self.records_per_day.items():
            self.stdout.write(f"  {date}: {count}")

        self.stdout.write("\nRecords fetched per month:")
        for (year, month), count in self.records_per_month.items():
            self.stdout.write(f"  {year}-{month:02d}: {count}")

        self.stdout.write(f"\nTotal records fetched: {self.total_records_fetched}")
        self.stdout.write(f"Total records processed: {self.total_records_processed}")

        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM apis_navmodel")
                total_records_in_db = cursor.fetchone()[0]
            self.stdout.write(f"\nTotal records in the database: {total_records_in_db}")
        except OperationalError as e:
            self.stdout.write(self.style.ERROR(f"Error counting records in database: {str(e)}"))

        if settings.DEBUG:
            self.stdout.write(self.style.WARNING(
                "\nWarning: DEBUG mode is enabled. Consider disabling it for better performance in production."))
