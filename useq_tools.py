#!/env/bin/python
"""USEQ tools"""

import sys
import argparse
import logging
import datetime

from typing import Optional, Dict, Any, List
from pathlib import Path
from genologics.lims import Lims
from logging.handlers import TimedRotatingFileHandler
from logging import StreamHandler
# Import modules
import utilities
import epp
import daemons
from config import Config

# Configure USEQTools main log

# Create a logger
logger = logging.getLogger('USEQTools')
logger.setLevel(logging.INFO)

# Create a TimedRotatingFileHandler for monthly rotation
# when='M': Rotate the log file every month
# interval=1: The interval is 1 unit of 'when' (i.e., every 1 month)
# backupCount=12: Keep up to 12 old log files
main_log_handler = TimedRotatingFileHandler(
    Config.LOG_FILE,
    when='M',
    interval=1,
    backupCount=12
)

#Console logger
stream_log_handler = StreamHandler()
stream_log_handler.setLevel(logging.INFO)

# Set the formatter for the handlers
formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

main_log_handler.setFormatter(formatter)
stream_log_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(main_log_handler)
logger.addHandler(stream_log_handler)

class USEQTools:
    """Main class for USEQ tools"""

    def __init__(self):
        """Initialize USEQ tools with LIMS connection."""
        try:
            self.lims = Lims(Config.LIMS_URI, Config.LIMS_USER, Config.LIMS_PW)
            logger.info("Successfully connected to LIMS")
        except Exception as e:
            logger.error(f"Failed to connect to LIMS: {e}")
            raise ConnectionError(f"LIMS connection failed: {e}")

    def setup_argument_parser(self) -> argparse.ArgumentParser:
        """Set up the main argument parser with all subcommands."""
        parser = argparse.ArgumentParser(
            description="USEQ Tools - Management utilities",
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        subparsers = parser.add_subparsers(
            dest='command_group',
            help='Available command groups'
        )

        # Add command groups
        self._add_utility_commands(subparsers)
        self._add_epp_commands(subparsers)
        self._add_daemon_commands(subparsers)

        return parser

    def _add_utility_commands(self, subparsers: argparse._SubParsersAction):
        """Add utility command parsers."""
        util_parser = subparsers.add_parser(
            'utilities',
            help="Utility functions for account management, data sharing, and reporting"
        )
        util_subparsers = util_parser.add_subparsers(dest='utility_command')

        # Account management
        self._add_account_commands(util_subparsers)

        # Data management
        self._add_data_commands(util_subparsers)

        # Reporting
        self._add_reporting_commands(util_subparsers)

        # Project management
        self._add_project_commands(util_subparsers)

    def _add_account_commands(self, subparsers: argparse._SubParsersAction):
        """Add account management commands."""
        # Manage accounts
        accounts_parser = subparsers.add_parser(
            'manage_accounts',
            help='Create, edit & retrieve accounts (labs)'
        )
        accounts_parser.add_argument(
            '-m', '--mode',
            choices=['create', 'edit', 'retrieve', 'batch_edit'],
            required=True,
            help='Operation mode'
        )
        accounts_parser.add_argument(
            '-c', '--csv',
            type=Path,
            help='Path to input or output CSV file'
        )
        accounts_parser.add_argument(
            '-a', '--account',
            help='Account name or ID (leave empty for create mode)'
        )
        accounts_parser.set_defaults(func=self.manage_accounts)

        # Client mail
        mail_parser = subparsers.add_parser(
            'client_mail',
            help='Send emails to USEQ clients'
        )
        mail_parser.add_argument(
            '-m', '--mode',
            choices=['all', 'labs', 'accounts'],
            required=True,
            help='Email mode'
        )
        mail_parser.add_argument(
            '-c', '--content',
            type=argparse.FileType('r'),
            help='Path to content file'
        )
        mail_parser.add_argument(
            '-n', '--name',
            help='Lab or account names (comma-separated)'
        )
        mail_parser.add_argument(
            '-a', '--attachment',
            type=Path,
            help='Path to attachment file'
        )
        mail_parser.set_defaults(func=self.client_mail)


        # Get researchers
        researchers_parser = subparsers.add_parser(
            'get_researchers',
            help='Get information for all researchers'
        )
        researchers_parser.set_defaults(func=self.get_researchers)

        # Get accounts
        get_accounts_parser = subparsers.add_parser(
            'get_accounts',
            help='Get information for all accounts'
        )
        get_accounts_parser.add_argument(
            '-o', '--output_file',
            type=argparse.FileType('w'),
            default=sys.stdout,
            help='Output file path (default: stdout)'
        )
        get_accounts_parser.set_defaults(func=self.get_accounts)

    def _add_data_commands(self, subparsers: argparse._SubParsersAction):
        """Add data management commands."""
        # Share data
        share_parser = subparsers.add_parser(
            'share_data',
            help='Encrypt and share datasets'
        )
        share_parser.add_argument(
            '-i', '--ids',
            help='Project ID(s) to share (comma-separated)'
        )
        share_parser.add_argument(
            '-u', '--username',
            help='Username to share data with'
        )
        share_parser.add_argument(
            '-d', '--dir',
            type=Path,
            help='Directory containing data to share'
        )
        share_parser.add_argument(
            '-f', '--fid',
            help='Override Flowcell ID from LIMS'
        )
        share_parser.add_argument(
            '-a', '--all_dirs_ont',
            action='store_true',
            help='Share all Nanopore directories'
        )
        share_parser.set_defaults(func=self.share_data)

        # Manage run IDs
        runids_parser = subparsers.add_parser(
            'manage_runids',
            help='Link or unlink run IDs for users'
        )
        runids_parser.add_argument(
            '-c', '--csv',
            type=argparse.FileType('r'),
            required=True,
            help='Path to CSV file'
        )
        runids_parser.add_argument(
            '-m', '--mode',
            choices=['link', 'unlink'],
            required=True,
            help='Operation mode'
        )
        runids_parser.set_defaults(func=self.manage_runids)

        # Link run results
        link_parser = subparsers.add_parser(
            'link_run_results',
            help='Link run results for a run ID'
        )
        link_parser.add_argument(
            '-i', '--runid',
            required=True,
            help='LIMS run ID'
        )
        link_parser.set_defaults(func=self.link_run_results)

    def _add_reporting_commands(self, subparsers: argparse._SubParsersAction):
        """Add reporting commands."""
        # Budget overview
        budget_parser = subparsers.add_parser(
            'budget_overview',
            help='Overview of costs for budget numbers'
        )
        budget_parser.add_argument(
            '-b', '--budgetnrs',
            required=True,
            help='Budget numbers to analyze'
        )
        budget_parser.add_argument(
            '-o', '--output_file',
            type=argparse.FileType('w'),
            default=sys.stdout,
            help='Output file path (default: stdout)'
        )
        budget_parser.set_defaults(func=self.budget_overview)

        # Sample report
        sample_parser = subparsers.add_parser(
            'sample_report',
            help='Generate sample report for project'
        )
        sample_parser.add_argument(
            '-i', '--project_id',
            required=True,
            help='Project ID'
        )
        sample_parser.set_defaults(func=self.sample_report)

        # Year overview
        year_parser = subparsers.add_parser(
            'year_overview',
            help='Overview of all USEQ projects by year'
        )
        year_parser.add_argument(
            '-y', '--year',
            help='Year to analyze (leave empty for all years)'
        )
        year_parser.add_argument(
            '-o', '--output',
            type=argparse.FileType('w'),
            default=sys.stdout,
            help='Output file path (default: stdout)'
        )
        year_parser.set_defaults(func=self.year_overview)

    def _add_project_commands(self, subparsers: argparse._SubParsersAction):
        """Add project management commands."""
        # Route project
        route_parser = subparsers.add_parser(
            'route_project',
            help='Route project samples to specific LIMS protocol'
        )
        route_parser.add_argument(
            '-i', '--project_id',
            required=True,
            help='LIMS project ID'
        )
        route_parser.add_argument(
            '-p', '--protocol_type',
            required=True,
            choices=[
                'ISOLATION', 'LIBPREP', 'POOLING', 'POOL QC',
                'ILLUMINA SEQUENCING', 'NANOPORE SEQUENCING', 'POST SEQUENCING'
            ],
            help='LIMS protocol type'
        )
        route_parser.set_defaults(func=self.route_project)

        # Finish run
        finish_parser = subparsers.add_parser(
            'finish_run',
            help='Mark run as finished'
        )
        finish_parser.add_argument(
            '-i', '--project_id',
            required=True,
            help='Project ID'
        )
        finish_parser.add_argument(
            '-f', '--fid',
            required=True,
            help='Flowcell ID'
        )
        finish_parser.add_argument(
            '-s', '--successful',
            action='store_true',
            help='Mark run as successful'
        )
        finish_parser.set_defaults(func=self.finish_run)

        # Update stats
        stats_parser = subparsers.add_parser(
            'update_stats',
            help='Update statistics database'
        )
        stats_parser.set_defaults(func=self.update_stats)

        # Update step UDF
        udf_parser = subparsers.add_parser(
            'update_step_udf',
            help='Update step UDF value'
        )
        udf_parser.add_argument(
            '-s', '--step',
            required=True,
            help='Step URI'
        )
        udf_parser.add_argument(
            '-n', '--name',
            required=True,
            help='UDF name'
        )
        udf_parser.add_argument(
            '-v', '--value',
            required=True,
            help='UDF value'
        )
        udf_parser.set_defaults(func=self.update_step_udf)

    def _add_epp_commands(self, subparsers: argparse._SubParsersAction):
        """Add EPP (External Process Protocol) commands."""
        epp_parser = subparsers.add_parser(
            'epp',
            help="Clarity EPP functions for LIMS integration"
        )
        epp_subparsers = epp_parser.add_subparsers(dest='epp_command')



        # Add other EPP commands with similar pattern...
        self._add_file_commands(epp_subparsers)
        self._add_workflow_commands(epp_subparsers)

    def _add_file_commands(self, subparsers: argparse._SubParsersAction):
        """Add file-related EPP commands."""
        # Modify samplesheet
        modify_parser = subparsers.add_parser(
            'modify_samplesheet',
            help='Modify samplesheet for different sequencers'
        )
        modify_parser.add_argument('-s', '--step', required=True, help='Step URI')
        modify_parser.add_argument('-a', '--aid', required=True, help='Artifact ID')
        modify_parser.add_argument(
            '-m', '--mode',
            choices=['rev', 'v1tov2'],
            required=True,
            help='Modification mode'
        )
        modify_parser.add_argument(
            '-o', '--output_file',
            type=argparse.FileType('w'),
            default=sys.stdout,
            help='Output file path'
        )
        modify_parser.set_defaults(func=self.modify_samplesheet)

        # Create samplesheet
        create_parser = subparsers.add_parser(
            'create_samplesheet',
            help='Create v2 samplesheet'
        )
        create_parser.add_argument('-s', '--step', required=True, help='Step URI')
        create_parser.add_argument(
            '-o', '--output_file',
            type=argparse.FileType('w'),
            default=sys.stdout,
            help='Output file path'
        )
        create_parser.add_argument('-t', '--type', help='Sample sheet type')
        create_parser.set_defaults(func=self.create_samplesheet)

        # Finance overview
        finance_parser = subparsers.add_parser(
            'finance_overview',
            help='Create finance overview for billing'
        )
        finance_parser.add_argument('-s', '--step', required=True, help='Step URI')
        finance_parser.add_argument(
            '-o', '--output_file',
            type=argparse.FileType('w'),
            default=sys.stdout,
            help='Output file path'
        )
        finance_parser.set_defaults(func=self.finance_overview)

        # Parse worksheet
        worksheet_parser = subparsers.add_parser(
            'parse_worksheet',
            help='Parse Excel worksheet'
        )
        worksheet_parser.add_argument('-s', '--step', required=True, help='Step URI')
        worksheet_parser.add_argument('-a', '--aid', required=True, help='Artifact ID')
        worksheet_parser.add_argument(
            '-o', '--output_file',
            type=argparse.FileType('w'),
            default=sys.stdout,
            help='Output file path'
        )
        worksheet_parser.add_argument(
            '-m', '--mode',
            choices=['illumina', 'ont', 'snp'],
            required=True,
            help='Parsing mode (affects available barcodes)'
        )
        worksheet_parser.set_defaults(func=self.parse_worksheet)

        # Create recipe
        recipe_parser = subparsers.add_parser(
            'create_recipe',
            help='Create NovaSeq run recipe'
        )
        recipe_parser.add_argument('-s', '--step', required=True, help='Step URI')
        recipe_parser.add_argument(
            '-o', '--output_file',
            type=argparse.FileType('w'),
            default=sys.stdout,
            help='Output file path'
        )
        recipe_parser.set_defaults(func=self.create_recipe)

    def _add_workflow_commands(self, subparsers: argparse._SubParsersAction):
        """Add workflow-related EPP commands."""
        # Group permissions
        perms_parser = subparsers.add_parser(
            'group_permissions',
            help='Check user group permissions for LIMS step'
        )
        perms_parser.add_argument('-s', '--step', required=True, help='Step URI')
        perms_parser.add_argument('-g', '--groups', required=True, help='Required groups')
        perms_parser.set_defaults(func=self.group_permissions)

        # Route artifacts
        route_parser = subparsers.add_parser(
            'route_artifacts',
            help='Route artifacts to next workflow step'
        )
        route_parser.add_argument('-s', '--step', required=True, help='Step URI')
        route_parser.add_argument(
            '-i', '--input',
            action='store_true',
            help='Use input artifact'
        )
        route_parser.set_defaults(func=self.route_artifacts)

        # Close projects
        close_parser = subparsers.add_parser(
            'close_projects',
            help='Close projects in specified step'
        )
        close_parser.add_argument('-s', '--step', help='Step URI')
        close_parser.add_argument('-p', '--pid', help='Project ID (overrides step URI)')
        close_parser.set_defaults(func=self.close_projects)

        # Check barcodes
        barcodes_parser = subparsers.add_parser(
            'check_barcodes',
            help='Check if barcodes are attached to samples'
        )
        barcodes_parser.add_argument('-s', '--step', required=True, help='Step URI')
        barcodes_parser.set_defaults(func=self.check_barcodes)

        # Chromium add-ons
        chromium_parser = subparsers.add_parser(
            'chromium_addons',
            help='Create Chromium add-on derived samples'
        )
        chromium_parser.add_argument('-s', '--step', required=True, help='Step URI')
        chromium_parser.set_defaults(func=self.chromium_addons)


        # Run status mail
        status_parser = subparsers.add_parser(
            'run_status',
            help='Send run status email'
        )
        status_parser.add_argument(
            '-m', '--mode',
            choices=['run_started', 'run_finished'],
            required=True,
            help='Status mode'
        )
        status_parser.add_argument(
            '-s', '--step_uri',
            help='Step URI that launched this script'
        )
        status_parser.set_defaults(func=self.run_status_mail)

    def _add_daemon_commands(self, subparsers: argparse._SubParsersAction):
        """Add daemon command parsers."""
        daemon_parser = subparsers.add_parser(
            'daemons',
            help="Background daemon scripts for monitoring and automation"
        )
        daemon_subparsers = daemon_parser.add_subparsers(dest='daemon_command')

        # Nextcloud monitor
        nc_parser = daemon_subparsers.add_parser(
            'nextcloud_monitor',
            help='Monitor NextCloud storage and send alerts'
        )
        nc_parser.set_defaults(func=self.nextcloud_monitor)

        # Manage runs
        runs_parser = daemon_subparsers.add_parser(
            'manage_runs',
            help='Manage sequencing run processing pipeline'
        )
        runs_parser.add_argument(
            '-d', '--skip_demux_check',
            action='store_true',
            help='Skip demultiplexing check (for low quality runs)'
        )
        runs_parser.set_defaults(func=self.manage_runs)

        # Run overview
        overview_parser = daemon_subparsers.add_parser(
            'run_overview',
            help='Update run overview for USEQ website'
        )
        overview_parser.add_argument(
            '-o', '--overview_file',
            default='overview.json',
            help='Output overview file path'
        )
        overview_parser.set_defaults(func=self.run_overview)

    # Utility command handlers
    def manage_accounts(self, args):
        """Handle account management operations."""
        try:
            utilities.useq_manage_accounts.run(
                self.lims, args.mode, args.csv, args.account
            )
        except Exception as e:
            logger.error(f"Account management failed: {e}")
            raise

    def client_mail(self, args):
        """Handle client email operations."""
        try:
            utilities.useq_client_mail.run(
                self.lims, Config.MAIL_SENDER, args.content,
                args.mode, args.attachment, args.name
            )
        except Exception as e:
            logger.error(f"Client mail failed: {e}")
            raise

    def share_data(self, args):
        """Handle data sharing operations."""
        try:
            utilities.useq_share_run.run(
                self.lims, args.ids, args.username, args.dir,
                args.fid, args.all_dirs_ont
            )
        except Exception as e:
            logger.error(f"Data sharing failed: {e}")
            raise

    def budget_overview(self, args):
        """Handle budget overview generation."""
        try:
            utilities.useq_budget_overview.run(
                self.lims, args.budgetnrs, args.output_file
            )
        except Exception as e:
            logger.error(f"Budget overview failed: {e}")
            raise

    def get_researchers(self, args):
        """Handle researcher information retrieval."""
        try:
            utilities.useq_get_researchers.run(self.lims)
        except Exception as e:
            logger.error(f"Get researchers failed: {e}")
            raise

    def get_accounts(self, args):
        """Handle account information retrieval."""
        try:
            utilities.useq_get_accounts.run(self.lims, args.output_file)
        except Exception as e:
            logger.error(f"Get accounts failed: {e}")
            raise

    def manage_runids(self, args):
        """Handle run ID management."""
        try:
            utilities.useq_manage_runids.run(self.lims, args.csv, args.mode)
        except Exception as e:
            logger.error(f"Run ID management failed: {e}")
            raise

    def link_run_results(self, args):
        """Handle run result linking."""
        try:
            utilities.useq_link_run_results.run(self.lims, args.runid)
        except Exception as e:
            logger.error(f"Link run results failed: {e}")
            raise

    def year_overview(self, args):
        """Handle year overview generation."""
        try:
            utilities.useq_year_overview.run(self.lims, args.year, args.output)
        except Exception as e:
            logger.error(f"Year overview failed: {e}")
            raise

    def route_project(self, args):
        """Handle project routing."""
        try:
            utilities.useq_route_project.run(
                self.lims, args.project_id, args.protocol_type
            )
        except Exception as e:
            logger.error(f"Project routing failed: {e}")
            raise

    def sample_report(self, args):
        """Handle sample report generation."""
        try:
            utilities.useq_sample_report.run(self.lims, args.project_id)
        except Exception as e:
            logger.error(f"Sample report failed: {e}")
            raise

    def finish_run(self, args):
        """Handle run finishing."""
        try:
            utilities.useq_finished_run.run(
                self.lims, args.project_id, args.fid, args.successful
            )
        except Exception as e:
            logger.error(f"Finish run failed: {e}")
            raise

    def update_stats(self, args):
        """Handle statistics update."""
        try:
            utilities.useq_update_stats_db.run(self.lims)
        except Exception as e:
            logger.error(f"Stats update failed: {e}")
            raise

    def update_step_udf(self, args):
        """Handle step UDF update."""
        try:
            utilities.useq_update_step_udf.run(
                self.lims, args.step, args.name, args.value
            )
        except Exception as e:
            logger.error(f"Step UDF update failed: {e}")
            raise

    # EPP command handlers
    def run_status_mail(self, args):
        """Handle run status email."""
        try:
            epp.useq_run_status_mail.run(
                self.lims, Config.MAIL_SENDER, Config.MAIL_ADMINS,
                args.mode, args.step_uri
            )
        except Exception as e:
            logger.error(f"Run status mail failed: {e}")
            raise

    def modify_samplesheet(self, args):
        """Handle samplesheet modification."""
        try:
            epp.useq_modify_samplesheet.run(
                self.lims, args.step, args.aid, args.output_file, args.mode
            )
        except Exception as e:
            logger.error(f"Samplesheet modification failed: {e}")
            raise

    def group_permissions(self, args):
        """Handle group permissions check."""
        try:
            epp.useq_group_permissions.run(self.lims, args.step, args.groups)
        except Exception as e:
            logger.error(f"Group permissions check failed: {e}")
            raise

    def finance_overview(self, args):
        """Handle finance overview generation."""
        try:
            epp.useq_finance_overview.run(self.lims, args.step, args.output_file)
        except Exception as e:
            logger.error(f"Finance overview failed: {e}")
            raise

    def route_artifacts(self, args):
        """Handle artifact routing."""
        try:
            epp.useq_route_artifacts.run(self.lims, args.step, args.input)
        except Exception as e:
            logger.error(f"Artifact routing failed: {e}")
            raise

    def close_projects(self, args):
        """Handle project closing."""
        try:
            epp.useq_close_projects.run(self.lims, args.step, args.pid)
        except Exception as e:
            logger.error(f"Project closing failed: {e}")
            raise

    def create_recipe(self, args):
        """Handle recipe creation."""
        try:
            epp.useq_create_recipe.run(self.lims, args.step, args.output_file)
        except Exception as e:
            logger.error(f"Recipe creation failed: {e}")
            raise

    def create_samplesheet(self, args):
        """Handle samplesheet creation."""
        try:
            epp.useq_create_samplesheet.run(
                self.lims, args.step, args.output_file, args.type
            )
        except Exception as e:
            logger.error(f"Samplesheet creation failed: {e}")
            raise

    def parse_worksheet(self, args):
        """Handle worksheet parsing."""
        try:
            epp.useq_parse_worksheet.run(
                self.lims, args.step, args.aid, args.output_file, args.mode
            )
        except Exception as e:
            logger.error(f"Worksheet parsing failed: {e}")
            raise

    def check_barcodes(self, args):
        """Handle barcode checking."""
        try:
            epp.useq_check_barcodes.run(self.lims, args.step)
        except Exception as e:
            logger.error(f"Barcode checking failed: {e}")
            raise

    def chromium_addons(self, args):
        """Handle Chromium add-ons."""
        try:
            epp.useq_chromium_addons.run(self.lims, args.step)
        except Exception as e:
            logger.error(f"Chromium add-ons failed: {e}")
            raise

    # Daemon command handlers
    def nextcloud_monitor(self, args):
        """Handle Nextcloud monitoring."""
        try:
            daemons.useq_nextcloud_monitor.run()
        except Exception as e:
            logger.error(f"Nextcloud monitoring failed: {e}")
            raise

    def manage_runs(self, args):
        """Handle run management."""
        try:
            daemons.useq_manage_runs.run(self.lims, args.skip_demux_check)
        except Exception as e:
            logger.error(f"Run management failed: {e}")
            raise

    def run_overview(self, args):
        """Handle run overview generation."""
        try:
            daemons.useq_run_overview.run(self.lims, args.overview_file)
        except Exception as e:
            logger.error(f"Run overview failed: {e}")
            raise

    def run(self):
        """Main entry point for the application."""
        parser = self.setup_argument_parser()
        args = parser.parse_args()

        if not hasattr(args, 'func'):
            parser.print_help()
            return 1

        try:
            args.func(args)
            return 0
        except Exception as e:
            logger.error(f"Command failed: {e}")
            return 1

def main():
    """Main entry point."""
    try:
        app = USEQTools()
        return app.run()
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
