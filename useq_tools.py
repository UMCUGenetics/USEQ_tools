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


class USEQTools:
    """Main class for USEQ Tools"""

    def __init__(self):
        """Initialize USEQ tools with LIMS connection."""
        try:
            self.lims = Lims(Config.LIMS_URI, Config.LIMS_USER, Config.LIMS_PW)
            logger.info("Successfully connected to LIMS")
        except Exception as e:
            logger.error(f"Failed to connect to LIMS: {e}")
            raise ConnectionError(f"LIMS connection failed: {e}")

    def _setup_argument_parser(self) -> argparse.ArgumentParser:

        """
        Set up the main argument parser with all subcommands.

        """
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
        """
        Internal function to add utility command parsers.

        Args:
            subparsers: ArgumentParser subparsers

        """
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
        """
        Internal function to add account management commands.

        Args:
            subparsers: ArgumentParser subparsers

        """
        # Manage accounts
        accounts_parser = subparsers.add_parser(
            'manage_accounts',
            help='Create, edit, retrieve and batch edit accounts (labs)'
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
        researchers_parser.add_argument(
            '-o', '--output_file',
            type=argparse.FileType('w'),
            default=sys.stdout,
            help='Output file path (default: stdout)'
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
        """
        Internal function to add data management commands.

        Args:
            subparsers: ArgumentParser subparsers

        """

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
        # link_parser = subparsers.add_parser(
        #     'link_run_results',
        #     help='Link run results for a run ID'
        # )
        # link_parser.add_argument(
        #     '-i', '--runid',
        #     required=True,
        #     help='LIMS run ID'
        # )
        # link_parser.set_defaults(func=self.link_run_results)

    def _add_reporting_commands(self, subparsers: argparse._SubParsersAction):
        """
        Internal function to add reporting commands.

        Args:
            subparsers: ArgumentParser subparsers

        """

        # Budget overview (old code, will rewrite when needed)
        # budget_parser = subparsers.add_parser(
        #     'budget_overview',
        #     help='Overview of costs for budget numbers'
        # )
        # budget_parser.add_argument(
        #     '-b', '--budgetnrs',
        #     required=True,
        #     help='Budget numbers to analyze'
        # )
        # budget_parser.add_argument(
        #     '-o', '--output_file',
        #     type=argparse.FileType('w'),
        #     default=sys.stdout,
        #     help='Output file path (default: stdout)'
        # )
        # budget_parser.set_defaults(func=self.budget_overview)

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
        sample_parser.add_argument(
            '-o', '--output_file',
            type=argparse.FileType('w'),
            default=sys.stdout,
            help='Output file path (default: stdout)'
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
        """
        Internal function to add project management commands.

        Args:
            subparsers: ArgumentParser subparsers

        """

        # Route project
        route_parser = subparsers.add_parser(
            'route_project',
            help='Route project samples to specific LIMS protocol'
        )
        route_parser.add_argument(
            '-i', '--project_id',
            required=True,
            help='Project LIMS ID'
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

        # Update stats (old code, will rewrite when needed)
        # stats_parser = subparsers.add_parser(
        #     'update_stats',
        #     help='Update statistics database'
        # )
        # stats_parser.set_defaults(func=self.update_stats)

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
        """
        Internal function to add EPP (External Process Protocol) commands.

        Args:
            subparsers: ArgumentParser subparsers

        """


        epp_parser = subparsers.add_parser(
            'epp',
            help="Clarity EPP functions for LIMS integration"
        )
        epp_subparsers = epp_parser.add_subparsers(dest='epp_command')

        # Add other EPP commands with similar pattern...
        self._add_file_commands(epp_subparsers)
        self._add_workflow_commands(epp_subparsers)

    def _add_file_commands(self, subparsers: argparse._SubParsersAction):
        """
        Internal function to add file-related EPP commands.

        Args:
            subparsers: ArgumentParser subparsers

        """

        # Modify samplesheet
        # modify_parser = subparsers.add_parser(
        #     'modify_samplesheet',
        #     help='Modify samplesheet for different sequencers'
        # )
        # modify_parser.add_argument('-s', '--step', required=True, help='Step URI')
        # modify_parser.add_argument('-a', '--aid', required=True, help='Artifact ID')
        # modify_parser.add_argument(
        #     '-m', '--mode',
        #     choices=['rev', 'v1tov2'],
        #     required=True,
        #     help='Modification mode'
        # )
        # modify_parser.add_argument(
        #     '-o', '--output_file',
        #     type=argparse.FileType('w'),
        #     default=sys.stdout,
        #     help='Output file path'
        # )
        # modify_parser.set_defaults(func=self.modify_samplesheet)

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
        # recipe_parser = subparsers.add_parser(
        #     'create_recipe',
        #     help='Create NovaSeq run recipe'
        # )
        # recipe_parser.add_argument('-s', '--step', required=True, help='Step URI')
        # recipe_parser.add_argument(
        #     '-o', '--output_file',
        #     type=argparse.FileType('w'),
        #     default=sys.stdout,
        #     help='Output file path'
        # )
        # recipe_parser.set_defaults(func=self.create_recipe)

    def _add_workflow_commands(self, subparsers: argparse._SubParsersAction):
        """
        Internal function to add workflow-related EPP commands.

        Args:
            subparsers: ArgumentParser subparsers

        """
        # Group permissions
        # perms_parser = subparsers.add_parser(
        #     'group_permissions',
        #     help='Check user group permissions for LIMS step'
        # )
        # perms_parser.add_argument('-s', '--step', required=True, help='Step URI')
        # perms_parser.add_argument('-g', '--groups', required=True, help='Required groups')
        # perms_parser.set_defaults(func=self.group_permissions)

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
        """
        Internal function to add daemon command parsers.

        Args:
            subparsers: ArgumentParser subparsers

        """

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
        runs_parser.set_defaults(func=self.manage_runs)

        # Run overview
        # overview_parser = daemon_subparsers.add_parser(
        #     'run_overview',
        #     help='Update run overview for USEQ website'
        # )
        # overview_parser.add_argument(
        #     '-o', '--overview_file',
        #     default='overview.json',
        #     help='Output overview file path'
        # )
        # overview_parser.set_defaults(func=self.run_overview)

    # Utility command handlers
    def manage_accounts(self, args):
        """
        LIMS Account management operations (edit, create, retrieve and batch_edit).

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

              - mode: The operation mode (edit/create/retrieve/batch_edit)
              - csv: Path to account CSV file
              - account: Specific LIMS account ID

        Raises:
            Exception: Re-raises any exceptions that occur during account management operations after logging the error.

        Examples:
            Retrieve an account::

                python useq_tools.py utilities manage_accounts --mode retrieve --csv ~/152.csv --account 152

            Edit an account::

                python useq_tools.py utilities manage_accounts --mode edit --csv ~/152.csv --account 152

            Create an account::

                python useq_tools.py utilities manage_accounts --mode create --csv ~/new_account.csv

            Batch edit multiple accounts::

                python useq_tools.py utilities manage_accounts --mode batch_edit --csv ~/accounts_to_edit.csv

        Note:
            In LIMS an 'Account' is named 'Lab'.


        """
        try:
            utilities.useq_manage_accounts.run(
                self.lims, args.mode, args.csv, args.account
            )
        except Exception as e:
            logger.error(f"Account management failed: {e}")
            raise

    def client_mail(self, args):
        """
        Send an email all active LIMS Researchers, specific Lab(s) or individual Researchers.

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - content: File object containing email subject and content. An example can be found under the resources folder.
                - mode: Operation mode ('all', 'accounts', or 'labs').
                - attachment: Optional path to file attachment.
                - name: Required for 'accounts' and 'labs' modes. Comma-separated list of account usernames or lab names.

            sender: Email address of the sender. Set in config.py.

        Raises:
            Exception: Re-raises any exceptions that occur during client email operations after logging the error.

        Examples:
            Send an email to all LIMS active Researchers::

               python useq_tools.py utilities --mode all --content /path/to/copy/of/client_mail_template.csv --attachment <optional attachment>

            Labs::

               python useq_tools.py utilities client_mail --mode labs --content /path/to/copy/of/client_mail_template.csv --attachment <optional attachment> --name labname1,labname2,labnameetc

            Accounts::

               python useq_tools.py utilities client_mail --mode accounts --content /path/to/copy/of/client_mail_template.csv --attachment <optional attachment> --name username1,username2,usernameetc


        Note:
            - In the context of this functionality 'accounts' means LIMS Researchers.
            - Only LIMS Researchers that are not archived (no longer active) are sent an email.
        """
        try:
            utilities.useq_client_mail.run(
                self.lims, Config.MAIL_SENDER, args.content,
                args.mode, args.attachment, args.name
            )
        except Exception as e:
            logger.error(f"Client mail failed: {e}")
            raise

    def share_data(self, args):
        """
        Share data (Illumina, ONT or other) with registered LIMS Researchers.

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - ids: Project LIMS ID(s).
                - username: LIMS Researcher username (e.g. u.seq).
                - dir: Optional directory to share.
                - fid: Optional flowcell ID, overwrites the flowcell ID found in LIMS.
                - all_dirs_ont: Flag determining packaging logic. If True, packages full data directories (e.g., fastq_pass.tar.gz). If False, attempts to package by individual barcode folders.

        Raises:
            Exception: Re-raises any exceptions that occur during data sharing operations after logging the error.

        Examples:
            Share Illumina sequencing run by Project LIMS ID::

                python useq_tools.py utilities share_data --ids <projectlimsid>

            Share Illumina sequencing run, but use the given flowcell ID to locate the run directory::

                python useq_tools.py utilities share_data --ids <projectlimsid> --fid <flowcellid>

            Share a directory with a user::

                python useq_tools.py utilities share_data --username <username> --dir </path/to/dir>

            Share an ONT sequencing run (all data directories)::

                python useq_tools.py utilities share_data --ids <projectlimsid> --all_dirs_ont

            Share an ONT sequencing run (only barcode directories)::

                python useq_tools.py utilities share_data --ids <projectlimsid>

        """

        try:
            utilities.useq_share_run.run(
                self.lims, args.ids, args.username, args.dir,
                args.fid, args.all_dirs_ont
            )
        except Exception as e:
            logger.error(f"Data sharing failed: {e}")
            raise

    #Old code, will rewrite when needed
    # def budget_overview(self, args):
    #     """Handle budget overview generation."""
    #     try:
    #         utilities.useq_budget_overview.run(
    #             self.lims, args.budgetnrs, args.output_file
    #         )
    #     except Exception as e:
    #         logger.error(f"Budget overview failed: {e}")
    #         raise

    def get_researchers(self, args):
        """
        Retrieve a list of all LIMS Researchers.

        Writes a semicolon-delimited list of LIMS researchers. Includes (in order) the following columns::

            1. LIMS ID              9. Billing Street
            2. First Name          10. Billing City
            3. Last Name           11. Billing State
            4. Email               12. Billing Country
            5. Username            13. Billing Postal Code
            6. Account Locked      14. Billing Institution
            7. Lab Name            15. Billing Department
            8. Lab ID

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - output_file: Optional output file, defaults to stdout.

        Raises:
            Exception: Re-raises any exceptions that occur during retrieval of researcher information.

        Examples:
            Write a list of all LIMS Researchers to stdout::

                python useq_tools.py utilities get_researchers

            Write a list of all LIMS Researchers to a file::

                python useq_tools.py utilities get_researchers --output_file <outputfile>

        """
        try:
            utilities.useq_get_researchers.run(self.lims, args.output_file)
        except Exception as e:
            logger.error(f"Get researchers failed: {e}")
            raise

    def get_accounts(self, args):
        """
        Retrieve a list of LIMS Labs (accounts).

        Writes a semicolon-delimited list of LIMS labs (accounts). Includes (in order) the following columns::

            1. Name                  8. Billing Institution   15. Shipping Institution
            2. LIMS ID               9. Billing Department    16. Shipping Department
            3. Billing Street       10. Shipping Street       17. Budget Numbers
            4. Billing City         11. Shipping City         18. VAT Number
            5. Billing State        12. Shipping State        19. Debtor Number
            6. Billing Country      13. Shipping Country      20. Supervisor Email
            7. Billing PostalCode   14. Shipping PostalCode   21. Finance Email

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - output_file: Optional output file, defaults to stdout.

        Raises:
            Exception: Re-raises any exceptions that occur during retrieval of account information.

        Examples:
            Write a list of all LIMS Labs to stdout::

                python useq_tools.py utilities get_accounts

            Write a list of all LIMS Labs to a file::

                python useq_tools.py utilities get_accounts --output_file <outputfile>

        """
        try:
            utilities.useq_get_accounts.run(self.lims, args.output_file)
        except Exception as e:
            logger.error(f"Get accounts failed: {e}")
            raise

    def manage_runids(self, args):
        """
        Link or unlink USEQ Portal Run IDs to (new or existing) LIMS Project IDs. The 'link' functionality is used when researchers
        request their first Run ID, otherwise this is handled by the USEQ Portal. The unlink functionality is used
        when a mistake was made (for whatever reason) during the linking of USEQ Portal Run ID and a LIMS Project ID.

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - csv: A csv file formatted like so::

                    DB_ID,USERNAME,LIMS_ID
                    <unique-portal-run-id>,<lims-username>,<optional-existing-lims-projectID>

                - mode:

                    - 'link' to link DB_ID's in the csv to new or existing (requires LIMS_ID) Project LIMS IDs.
                    - 'unlink' to unlink DB_ID's in the csv from Project LIMS IDs.

        Raises:
            Exception: Re-raises any exceptions that occur during the link or unlink operations.

        Examples:
            Link a portal run id to a new Project LIMS ID::

                python useq_tools.py utilities manage_runids --mode link --csv <csv_file>

            Unlink a portal run id from an existing Project LIMS ID::

                python useq_tools.py utilities manage_runids --mode unlink --csv <csv_file>

        """
        try:
            utilities.useq_manage_runids.run(self.lims, args.csv, args.mode)
        except Exception as e:
            logger.error(f"Run ID management failed: {e}")
            raise

    #Old code used when setting up the portal database. For new runs this functionality is handled by share_run.
    #Will only update this if needed in the future.
    # def link_run_results(self, args):
    #     """Handle run result linking."""
    #     try:
    #         utilities.useq_link_run_results.run(self.lims, args.runid)
    #     except Exception as e:
    #         logger.error(f"Link run results failed: {e}")
    #         raise

    def year_overview(self, args):
        """
        Creates a summary of all Project LIMS IDs for a given year or for all years.

        The overview contains the following columns (semicolon-delimited)::

            1. Year         4. Sample Type
            2. Platform     5. Runs (nr runs)
            3. Run Type     6. Samples (nr samples)

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - year: The four-digit year (e.g., '2025') to filter projects based on their closed date. If None, projects from all years are processed.
                - output: Optional output file, defaults to stdout.

        Raises:
            Exception: Re-raises any exceptions that occur during the creation of the year overview.

        Examples:
            Create a year overview for the year 2024 and write it to an output file::

                python useq_tools.py utilities year_overview --year 2024 --output <output_file>

            Create a year overview for all years and write it to sdtout::

                python useq_tools.py utilities year_overview

        """
        try:
            utilities.useq_year_overview.run(self.lims, args.year, args.output)
        except Exception as e:
            logger.error(f"Year overview failed: {e}")
            raise

    def route_project(self, args):
        """
        Route all the samples in a LIMS Project to a specific protocol step.

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - project_id: Project LIMS ID
                - protocol_type: The type of protocol to route samples to. Valid values are: 'ISOLATION', 'LIBPREP', 'POOLING', 'POOL QC', 'ILLUMINA SEQUENCING', 'NANOPORE SEQUENCING', or 'POST SEQUENCING'.

        Raises:
            Exception: Re-raises any exceptions that occur during routing of the samples.

        Examples:
            Route all samples in a LIMS project to the appropriate library prep step::

                python useq_tools.py utilities route_project --project_id <Project LIMS ID> --protocol_type 'LIBPREP'

        Warning:
            Does not remove the samples from the current step. You will have to do this manually in the LIMS.
        """
        try:
            utilities.useq_route_project.run(
                self.lims, args.project_id, args.protocol_type
            )
        except Exception as e:
            logger.error(f"Project routing failed: {e}")
            raise

    def sample_report(self, args):
        """
        Create a simple sample report generation for a specific Project LIMS ID.

        Writes a simple sample report containing (per sample)::

            Isolated conc. (ng/ul), Pre library prep conc. (ng/ul), RIN, Post library prep conc. (ng/ul)

        And per pool (if applicable)::

            Library conc. (ng/ul), Average length (bp)

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - project_id: Project LIMS ID
                - output_file: Optional output file, defaults to stdout.

        Raises:
            Exception: Re-raises any exceptions that occur during the generation of the sample report.

        Examples:
            Create a sample report for a specific project ID and write it to file::

                python useq_tools.py utilities sample_report --project_id <Project LIMS ID> --output_file <output_file>

        Note:
            This functionality is mainly used in :meth:`share_data` and only incidentally manually.
        """
        try:
            utilities.useq_sample_report.run(self.lims, args.project_id, args.output_file)
        except Exception as e:
            logger.error(f"Sample report failed: {e}")
            raise

    def finish_run(self, args):
        """
        Processes artifacts in the "Process Raw Data" queue for a
        specific project, advances them through workflow stages, and updates the
        sequencing success status in both LIMS and the portal database.
        This should always be run directly after sharing a run successfully.

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - project_id: Project LIMS ID.
                - flowcell_id: The flowcell identifier associated with the sequencing run.
                - successful: Boolean indicating whether the sequencing was successful.

        Raises:
            Exception: Re-raises any exceptions that occur while finishing the run.

        Examples:
            Move project to next step in LIMS and set status to succesful (default)::

                python useq_tools.py utilities finish_run --project_id <project_id> --flowcell_id <flowcell_id>

            Move project to next step in LIMS and set status to failed::

                python useq_tools.py utilities finish_run --project_id <project_id> --flowcell_id <flowcell_id> --successful False

        """
        try:
            utilities.useq_finished_run.run(
                self.lims, args.project_id, args.fid, args.successful
            )
        except Exception as e:
            logger.error(f"Finish run failed: {e}")
            raise

    #Old code used when yield_r1 and yield_r2 were added to the IlluminaSequencingStats portal table.
    #Will only update this if needed in the future.
    # def update_stats(self, args):
    #     """Handle statistics update."""
    #     try:
    #         utilities.useq_update_stats_db.run(self.lims)
    #     except Exception as e:
    #         logger.error(f"Stats update failed: {e}")
    #         raise

    def update_step_udf(self, args):
        """
        Can be used to change the value belonging to a LIMS Step UDF (e.g. wrong flowcell ID).

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - step: URI of the step to update.
                - name: Name of the UDF to update.
                - value: New value for the UDF.

        Raises:
            Exception: Re-raises any exceptions that occur while updating the step UDF.

        Examples:
            Update the 'Flow Cell ID' of a a 'USEQ - NovaSeq X Run'-step:

            .. parsed-literal::

                python useq_tools.py utilities update_step_udf --step |lims_uri|/api/v2/steps/<STEP_ID> --name 'Flow Cell ID' --value 'NEWFLOWCELLID'

        Note:
            To get the STEP_ID for the example above go to the step in the LIMS and copy the number after |lims_uri|/clarity/work-complete/.
            For most steps add 24- in front of this number, for pooling steps add 122-.

        """
        try:
            utilities.useq_update_step_udf.run(
                self.lims, args.step, args.name, args.value
            )
        except Exception as e:
            logger.error(f"Step UDF update failed: {e}")
            raise

    # EPP command handlers
    def run_status_mail(self, args):
        """
        Sends a run started or run finished email.

        The 'run_started' mode is (currently) triggered by the following LIMS steps:

            - USEQ - NextSeq2000 Run
            - USEQ - iSeq Run
            - USEQ - NovaSeq X Run

        The 'run_finished' functionality is run in the useq_route_artifacts module when analysis was requested
        and/or the run platform is '60 SNP NimaGen panel' or 'Chromium X'. It will generate a Trello card for
        UBEC.

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - mode: Choose from 'run_started' or 'run_finished'.
                - step: LIMS step uri

        Raises:
            Exception: Re-raises any exceptions that occur when sending a run status email.

        Examples:
            Send a run started email manually:

            .. parsed-literal::

                python useq_tools.py epp run_status --step |lims_uri|/api/v2/steps/<STEP_ID> --mode run_started

            Send a run finished email manually (not supported yet)::

                todo

        Note:
            To get the STEP_ID for the example above go to the step in the LIMS and copy the number after |lims_uri|/clarity/work-complete/.
            For most steps add 24- in front of this number, for pooling steps add 122-.

        """
        try:
            epp.useq_run_status_mail.run(
                self.lims, Config.MAIL_SENDER, Config.MAIL_ADMINS,
                args.mode, args.step_uri
            )
        except Exception as e:
            logger.error(f"Run status mail failed: {e}")
            raise

    # Old functionality now handled by create_samplesheet and/or the manage_runs script
    # def modify_samplesheet(self, args):
    #     """Handle samplesheet modification."""
    #     try:
    #         epp.useq_modify_samplesheet.run(
    #             self.lims, args.step, args.aid, args.output_file, args.mode
    #         )
    #     except Exception as e:
    #         logger.error(f"Samplesheet modification failed: {e}")
    #         raise

    # Old functionality used in the early LIMS days when sample submission was manual. Both USEQ and Dx
    # no longer manually submit their samples, removing the risk of users from the wrong group uploading samples in our workflow.
    # def group_permissions(self, args):
    #     """Handle group permissions check."""
    #     try:
    #         epp.useq_group_permissions.run(self.lims, args.step, args.groups)
    #     except Exception as e:
    #         logger.error(f"Group permissions check failed: {e}")
    #         raise

    def finance_overview(self, args):
        """
        Create a billing list for all projects queued in the 'USEQ - Ready for billing' step.

        This function is usually started during the 'USEQ - Ready for billing' step, but can also be started manually as shown in the example.
        The output contains the following columns (in order)::

            1. errors                        17. plate_personell_costs         33. lims_libraryprep
            2. pool_name                     18. sequencing_step_costs         34. lims_isolation
            3. project_name                  19. sequencing_personell_costs    35. requested_analysis
            4. project_id                    20. analysis_step_costs           36. sequencing_succesful
            5. open_date                     21. analysis_personell_costs      37. description
            6. contact_name                  22. total_step_costs              38. order_number
            7. contact_email                 23. total_personell_costs         39. comments_and_agreements
            8. account                       24. billing_institute             40. deb_nr
            9. project_budget_number         25. billing_department            41. vat_nr
            10. sample_type                  26. billing_street                42. nr_samples_isolated
            11. nr_samples                   27. billing_postalcode            43. nr_samples_prepped
            12. isolation_step_costs         28. billing_city                  44. nr_samples_sequenced
            13. isolation_personell_costs    29. billing_country               45. nr_samples_analyzed
            14. libraryprep_step_costs       30. requested_runtype             46. nr_lanes
            15. libraryprep_personell_costs  31. lims_runtype
            16. plate_step_costs             32. requested_libraryprep

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - step: LIMS Step uri.
                - output_file: Optional output file, defaults to stdout.

        Raises:
            Exception: Re-raises any exceptions that occur when generating the finance overview.

        Examples:
            (re)create a finance overview from a specific billing step and write it to a file:

            .. parsed-literal::

                python useq_tools.py epp finance_overview --step |lims_uri|/api/v2/steps/<STEP_ID> --output_file <output_file>

        Note:
            To get the STEP_ID for the example above go to the step in the LIMS and copy the number after |lims_uri|/clarity/work-complete/.
            For most steps add 24- in front of this number, for pooling steps add 122-.
        """
        try:
            epp.useq_finance_overview.run(self.lims, args.step, args.output_file)
        except Exception as e:
            logger.error(f"Finance overview failed: {e}")
            raise

    def route_artifacts(self, args):
        """
        Routes all input or output artifacts in a LIMS step to the appropriate next workflow stages based on current step and sample properties.
        This function is usually run at the end of the last step in a protocol. Depending on the step either input or output artifacts
        are routed. Can also be run manually as seen in the example below.

        The following LIMS (active) steps route their input artifacts:

            - USEQ - Bioanalyzer QC DNA
            - USEQ - Aggregate QC (Library Pooling)
            - USEQ - Post LibPrep QC
            - USEQ - Qubit QC
            - USEQ - NextSeq2000 Run
            - USEQ - iSeq Run
            - USEQ - NovaSeq X Run
            - USEQ - Nanopore Run v2

        The following LIMS (active) steps route their output artifacts:

            - USEQ - Isolation v2
            - USEQ - BCL to FastQ
            - USEQ - Library Pooling
            - USEQ - Chromium X Run

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - step: URI of the current step.
                - input: If True, route input artifacts; if False, route output artifacts.

        Raises:
            Exception: Re-raises any exceptions that occur during artifact routing.

        Examples:
            Route the input artifacts of a step to the next step:

            .. parsed-literal::

                python useq_tools.py epp route_artifacts --step |lims_uri|/api/v2/steps/<STEP_ID> --input True

            Route the output artifacts of a step to the next step:

            .. parsed-literal::

                python useq_tools.py epp route_artifacts --step |lims_uri|/api/v2/steps/<STEP_ID> --input False

        Note:
            To get the STEP_ID for the example above go to the step in the LIMS and copy the number after |lims_uri|/clarity/work-complete/.
            For most steps add 24- in front of this number, for pooling steps add 122-.

        """
        try:
            epp.useq_route_artifacts.run(self.lims, args.step, args.input)
        except Exception as e:
            logger.error(f"Artifact routing failed: {e}")
            raise

    def close_projects(self, args):
        """
        Closes LIMS Projects by ID or by the LIMS Step that they're currently in.
        If a project ID is provided, closes that specific project. Otherwise, closes all
        projects associated with samples in the specified step. This last functionality is used
        at the end of the 'USEQ - Ready for billing' step.

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - step: URI of the step (required if project_id is not provided).
                - pid: Project LIMS ID of a specific project to close (optional).

        Raises:
            Exception: Re-raises any exceptions that occur during closing of one or more projects.

        Examples:
            Close a specific project by project ID::

                python useq_tools.py epp close_projects -p <Project LIMS ID>

            Close all projects in a LIMS step:

            .. parsed-literal::

                python useq_tools.py epp close_projects --step |lims_uri|/api/v2/steps/<STEP_ID>

        Note:
            To get the STEP_ID for the example above go to the step in the LIMS and copy the number after |lims_uri|/clarity/work-complete/.
            For most steps add 24- in front of this number, for pooling steps add 122-.

        """
        try:
            epp.useq_close_projects.run(self.lims, args.step, args.pid)
        except Exception as e:
            logger.error(f"Project closing failed: {e}")
            raise

    # Old functionality used for the NovaSeq6000. No longer used and code not updated.
    # def create_recipe(self, args):
    #     """Handle recipe creation."""
    #     try:
    #         epp.useq_create_recipe.run(self.lims, args.step, args.output_file)
    #     except Exception as e:
    #         logger.error(f"Recipe creation failed: {e}")
    #         raise

    def create_samplesheet(self, args):
        """
        Creates a samplesheet compatible with all modern Illumina sequencers. This script is currently run in the following
        (active) LIMS protocol steps:

            - USEQ - Denature, Dilute and Load iSeq
            - USEQ - Denature, Dilute and Load NovaSeqX
            - USEQ - Denature, Dilute and Load NextSeq2000

        For each sample in the step it fills the following BCLConvert_Data samplesheet fields (in order)::

            1. Lane                     5. Sample_Project
            2. Sample_ID                6. OverrideCycles
            3. index                    7. BarcodeMismatchesIndex1
            4. index2 (if applicable)   8. BarcodeMismatchesIndex2 (if applicable)

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - step: URI of the step.
                - output_file: Optional output file, defaults to stdout.

        Raises:
            Exception: Re-raises any exceptions that occur during samplesheet creation.

        Examples:
            Create a samplesheet and write it to a file:

            .. parsed-literal::

                python useq_tools.py epp create_samplesheet --step |lims_uri|/api/v2/steps/<STEP_ID> --output_file SampleSheet.csv

        Note:
            To get the STEP_ID for the example above go to the step in the LIMS and copy the number after |lims_uri|/clarity/work-complete/.
            For most steps add 24- in front of this number, for pooling steps add 122-.

        """
        try:
            epp.useq_create_samplesheet.run(
                self.lims, args.step, args.output_file
            )
        except Exception as e:
            logger.error(f"Samplesheet creation failed: {e}")
            raise

    def parse_worksheet(self, args):
        """
        Parses an Excel worksheet, depending on the LIMS protocol step it expects specific columns:

            - nr: Incremental sample number (required).
            - container name: Tube/Plate name (used in: 'USEQ - Isolation v2').
            - sample: Sample name (required).
            - pre conc ng/ul: Pre-library prep concentration (used in: 'USEQ - Isolation v2', 'USEQ - Pre LibPrep QC').
            - RIN: RNA Integrity Number (optional in: 'USEQ - Pre LibPrep QC').
            - barcode nr: Barcode number (used in: 'USEQ - LibPrep Illumina', 'USEQ - LibPrep Nanopore').
            - post conc ng/ul: Post-library prep concentration (used in: 'USEQ - Post LibPrep QC').
            - size: Fragment size (used in: 'USEQ - Post LibPrep QC').

        This script is almost never run manually (except in testing).

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - step: URI of the step
                - aid: Artifact ID (ID of the worksheet artifact in LIMS)
                - output_file: Optional output file (used for logging), defaults to stdout.
                - mode: illumina or ont

        Raises:
            Exception: Re-raises any exceptions that occur during worksheet parsing.

        Examples:
            Process a worksheet for an illumina protocol step:

            .. parsed-literal::

                python useq_tools.py epp parse_worksheet --aid <WORKSHEET-ARTIFACT-ID> --step |lims_uri|/api/v2/steps/<STEP_ID> --output_file <LOGFILE-ARTIFACT-ID> --mode illumina

        Note:
            - **STEP_ID**: To get the STEP_ID for the example above go to the step in the LIMS and copy the number after |lims_uri|/clarity/work-complete/.
              For most steps add 24- in front of this number, for pooling steps add 122-.
            - **WORKSHEET-ARTIFACT-ID**: Go to |lims_uri|/api/v2/steps/<STEP_ID>/details. Look up an input-output-map where the output artifact type is 'ResultFile' (you'll find multiple).
              Find the output artifact with the lowest limsid, this is your WORKSHEET-ARTIFACT-ID.
            - **LOGFILE-ARTIFACT-ID**: Go to |lims_uri|/api/v2/steps/<STEP_ID>/details. Find the output artifact with the highest limsid.

        """
        try:
            epp.useq_parse_worksheet.run(
                self.lims, args.step, args.aid, args.output_file, args.mode
            )
        except Exception as e:
            logger.error(f"Worksheet parsing failed: {e}")
            raise

    def check_barcodes(self, args):
        """
        Validates that each sample in the step has a reagent label (barcode) assigned.
        This script is run in the following LIMS steps:

            - USEQ - LibPrep Nanopore
            - USEQ - LibPrep Illumina

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - step: URI of the step

        Raises:
            Exception: Re-raises any exceptions that occur during barcode checking.

        Examples:
            Check barcodes in a library prep step:

            .. parsed-literal::

                python useq_tools.py epp check_barcodes --step |lims_uri|/api/v2/steps/<STEP_ID>

        Note:
            To get the STEP_ID for the example above go to the step in the LIMS and copy the number after |lims_uri|/clarity/work-complete/.
            For most steps add 24- in front of this number, for pooling steps add 122-.

        """
        try:
            epp.useq_check_barcodes.run(self.lims, args.step)
        except Exception as e:
            logger.error(f"Barcode checking failed: {e}")
            raise

    def chromium_addons(self, args):
        """
        This script generates new names for derived samples by appending add-on
        suffixes (BCR, TCR, CSP, CRISPR) based on which processing steps are enabled
        in the step's UDFs. This script is used in the following LIMS steps:

            - USEQ - Chromium X Cell Suspension & QC

        Args:
            args (argparse.Namespace): Contains command-line arguments including:

                - step: URI of the step

        Raises:
            Exception: Re-raises any exceptions that occur during renaming of derived samples.

        Examples:
            Rename add-on derived samples:

            .. parsed-literal::

                python useq_tools.py epp chromium_addons --step |lims_uri|/api/v2/steps/<STEP_ID>

        Note:
            To get the STEP_ID for the example above go to the step in the LIMS and copy the number after |lims_uri|/clarity/work-complete/.
            For most steps add 24- in front of this number, for pooling steps add 122-.

        """
        try:
            epp.useq_chromium_addons.run(self.lims, args.step)
        except Exception as e:
            logger.error(f"Chromium add-ons failed: {e}")
            raise

    # Daemon command handlers
    def nextcloud_monitor(self, args):
        """
        Checks storage usage for both raw data and manual directories,
        sending separate reports for each.

        Args:
            None

        Raises:
            Exception: Re-raises any exceptions that occur while creating the Nextcloud usage report.

        Examples:
            Create and mail a report for the raw_data & processed_data directories on Nextcloud::

                python useq_tools.py daemons nextcloud_monitor
        """
        try:
            daemons.useq_nextcloud_monitor.run()
        except Exception as e:
            logger.error(f"Nextcloud monitoring failed: {e}")
            raise

    def manage_runs(self, args):
        """
        This script handles the demultiplexing, transfer to nextcloud and archiving of
        Illumina sequencing runs. It currently runs on the 'apenboom' server every 10 minutes (see crontab -ls).

        A summary of what it's designed to do:

        1. Run Detection & Initialization

           - Monitors multiple sequencing machines for completed runs (indicated by RTAComplete.txt)
           - Retrieves or locates sample sheets from LIMS or run directory
           - Parses run metadata from XML files (RunInfo.xml, RunParameters.xml)
           - Implements locking mechanism to prevent concurrent processing

        2. Demultiplexing

           - Creates project-specific sample sheets with proper index orientations
           - Tests both forward and reverse complement index configurations
           - Validates demultiplexing quality by checking undetermined read ratios
           - Runs BCL Convert to generate FASTQ files from base call files
           - Adds flowcell IDs to FASTQ filenames for traceability

        3. Quality Control & Statistics

           - Generates run statistics using InterOp tools
           - Creates quality visualizations (intensity plots, base percentage, Q-score heatmaps)
           - Runs FastQC on all FASTQ files
           - Generates MultiQC reports for consolidated quality metrics
           - Consolidates per-project statistics into run-wide reports

        4. Data Distribution

           - Nextcloud: Uploads FASTQ files (or BCL data if demux fails) with MD5 checksums
           - HPC Storage: Transfers run data with selective file inclusion based on analysis needs
           - Archive: Long-term storage with compressed run data
           - Handles project-specific upload requirements (e.g., SNP fingerprinting)

        5. State Management

           - Tracks processing status in JSON file (status.json)
           - Records completion of: demultiplexing, statistics generation, transfers, archiving
           - Enables resumption after failures without repeating completed steps
           - Supports both per-project and run-level status tracking

        6. Error Handling & Notification

           - Comprehensive logging to run-specific log files
           - Email notifications with detailed statistics and quality plots
           - Graceful degradation (falls back to BCL-only mode if demultiplexing fails)
           - Proper cleanup of temporary files after successful completion

        The processing flow (in short):

            1. Run Complete
            2. Parse Sample Sheet
            3. Demultiplex (per project)
            4. Generate Statistics
            5. Transfer to Nextcloud
            6. Transfer to HPC
            7. Archive
            8. Cleanup
            9. Email Notification

        Args:
            args: Description of the arguments passed to the method.

        Raises:
            Exception: Re-raises any exceptions that occur during processing.

        Examples:
            Start the run processing daemon::

                python useq_tools daemons manage_runs
        """
        try:
            daemons.useq_manage_runs.run(self.lims)
        except Exception as e:
            logger.error(f"Run management failed: {e}")
            raise

    # Old functionality used for the previous overview website. This functionality is now handled by the useq portal website.
    # Code is no longer used and thus not updated.
    # def run_overview(self, args):
    #     """Handle run overview generation."""
    #     try:
    #         daemons.useq_run_overview.run(self.lims, args.overview_file)
    #     except Exception as e:
    #         logger.error(f"Run overview failed: {e}")
    #         raise

    def _run(self):
        """Main entry point for the application."""
        parser = self._setup_argument_parser()
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

def _main():
    """Main entry point."""
    # Configure USEQTools main log


    try:
        app = USEQTools()
        return app._run()
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        return 1


if __name__ == "__main__":
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
    sys.exit(_main())
