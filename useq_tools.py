#!/env/bin/python
"""USEQ tools"""

import sys
import argparse


from genologics.lims import Lims

# import resources
import utilities
import epp
import daemons
from config import Config


#Commandline utility Functions
def manage_accounts(args):
    """Create,Edit,Retrieve accounts (labs)"""
    utilities.useq_manage_accounts.run(lims, args.mode, args.csv, args.account)

def client_mail(args):
    """Send email to all specific USEQ clients, all clients belonging to an account or a single specific client."""
    utilities.useq_client_mail.run(lims, config.MAIL_SENDER, args.content, args.mode, args.attachment, args.name)

def share_data(args):
    """Encrypt and Share one or more datasets"""
    utilities.useq_share_run.run(lims, args.ids, args.username, args.dir, args.fid, args.all_dirs_ont)

def budget_overview(args):
    utilities.useq_budget_overview.run(lims, args.budgetnrs, args.output_file)

def get_researchers(args):
    utilities.useq_get_researchers.run(lims)

def get_accounts(args):
    utilities.useq_get_accounts.run(lims, args.output_file)

def manage_runids(args):
    utilities.useq_manage_runids.run(lims, args.csv, args.mode)

def link_run_results(args):
    utilities.useq_link_run_results.run(lims, args.runid)

def year_overview(args):
    utilities.useq_year_overview.run(lims, args.year, args.output)

def route_project(args):
    utilities.useq_route_project.run(lims, args.project_id, args.protocol_type)

def sample_report(args):
    utilities.useq_sample_report.run(lims,args.project_id)

def finish_run(args):
    utilities.useq_finished_run.run(lims,args.project_id, args.fid, args.succesful)

def update_stats(args):
    utilities.useq_update_stats_db.run(lims)

def update_step_udf(args):
    utilities.useq_update_step_udf.run(lims,args.step, args.name, args.value)

#Clarity epp scripts
def run_status_mail(args):
    """Send run started mail"""
    epp.useq_run_status_mail.run(lims, config.MAIL_SENDER, config.MAIL_ADMINS, args.mode ,args.step_uri)

def modify_samplesheet(args):
    """Reverse complements the barcodes in a samplesheet"""
    epp.useq_modify_samplesheet.run(lims, args.step, args.aid, args.output_file, args.mode)

def group_permissions(args):
    """Checks if a user trying to execute a LIMS step is part of the specified group(s)"""
    epp.useq_group_permissions.run(lims,args.step, args.groups)

def finance_overview(args):
    """Creates a finance overview, used for billing, for all runs in the current step"""
    epp.useq_finance_overview.run(lims, args.step, args.output_file)

def route_artifacts(args):
    """Route artifacts to the appropriate step in a workflow"""
    epp.useq_route_artifacts.run(lims, args.step, args.input)

def close_projects(args):
    """Close all projects included in the current step"""
    epp.useq_close_projects.run(lims, args.step, args.pid)

def create_recipe(args):
    """Create Novaseq run recipe"""
    epp.useq_create_recipe.run(lims, args.step,args.output_file)

def create_samplesheet(args):
    """Create generic v2 samplesheet"""
    epp.useq_create_samplesheet.run(lims, args.step,args.output_file, args.type)

def parse_worksheet(args):
    """Parse xslx worksheet"""
    epp.useq_parse_worksheet.run(lims, args.step, args.aid, args.output_file, args.mode)

def check_barcodes(args):
    """Check if barcodes are attached to samples in step"""
    epp.useq_check_barcodes.run(lims, args.step)

def chromium_addons(args):
    """"Create add-on derived samples"""
    epp.useq_chromium_addons.run(lims,args.step)

#Daemon scripts
def nextcloud_monitor(args):
    """Is intended to run as a daemon to check the space remaining on the Nextcloud storage"""
    daemons.useq_nextcloud_monitor.run()

def manage_runs(args):
    """Script responsible for starting conversion, transfer, cleanup and archiving of sequencing runs"""
    daemons.useq_manage_runs.run(lims, args.skip_demux_check)

def run_overview(args):
    """Creates json file intended for the USEQ-Overview website"""
    daemons.useq_run_overview.run(lims, args.overview_file)

if __name__ == "__main__":
    global lims

    # Setup lims connection
    lims = Lims(Config.LIMS_URI, Config.LIMS_USER, Config.LIMS_PW)

    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()

    #Utility parsers
    parser_utilities = subparser.add_parser('utilities',help="Utility functions: manage_accounts, client_mail, share_data, budget_overview , manage_runids,link_run_results, get_researchers, year_overview")
    subparser_utilities = parser_utilities.add_subparsers()

    parser_manage_accounts = subparser_utilities.add_parser('manage_accounts', help='Create, Edit & Retrieve accounts (labs)')
    parser_manage_accounts.add_argument('-m','--mode',choices=['create','edit','retrieve','batch_edit'])
    parser_manage_accounts.add_argument('-c','--csv', help='Path to input or output csv file')
    parser_manage_accounts.add_argument('-a','--account', help='Account name or ID. Leave empty for mode "create"', default=None)
    parser_manage_accounts.set_defaults(func=manage_accounts)

    parser_client_mail = subparser_utilities.add_parser('client_mail', help='Send email to all specific USEQ users, all clients belonging to an account or a single specific client.')
    parser_client_mail.add_argument('-m','--mode',choices=['all','labs','accounts'])
    parser_client_mail.add_argument('-c','--content', help='Path to content file (see resources for example)', nargs='?' ,type=argparse.FileType('r'))
    parser_client_mail.add_argument('-n','--name', help='Lab or Account name(s) separated by comma. Leave empty for mode "all" or "batch_edit"')
    parser_client_mail.add_argument('-a','--attachment', help='Path to attachment file')
    parser_client_mail.set_defaults(func=client_mail)

    parser_share_data = subparser_utilities.add_parser('share_data', help='Shares raw data allready on Nextcloud (by ID) or uploads data to nextcloud and then shares it (email and dir).')
    parser_share_data.add_argument('-i', '--ids', help='One or more Project ID(s) to share, separated by comma.')
    parser_share_data.add_argument('-u', '--username', help='Username to share data with.', default=None)
    parser_share_data.add_argument('-d', '--dir', help='Directory containing data to share.', default=None)
    parser_share_data.add_argument('-f', '--fid', help='Overrides the Flowcell ID found in the LIMS, ONLY use when the Flowcell ID in the LIMS is wrong (ROOOBIIIIN!!).', default=None)
    parser_share_data.add_argument('-a', '--all_dirs_ont', help='Share all Nanopore dirs. Only works for Nanopore data AND ignores the check for barcode dirs being present. Using this option can result in a lot of data being shared!!', default=False)
    parser_share_data.set_defaults(func=share_data)

    parser_budget_ovw = subparser_utilities.add_parser('budget_overview', help='Get an overview of all costs booked to supplied budget numbers.')
    parser_budget_ovw.add_argument('-o','--output_file',  nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='Output file path (default=stdout)')
    parser_budget_ovw.add_argument('-b', '--budgetnrs', required=True)
    parser_budget_ovw.set_defaults(func=budget_overview)

    parser_manage_runids = subparser_utilities.add_parser('manage_runids', help='Link or unlink one or multiple runIDs for a user.')
    parser_manage_runids.add_argument('-c', '--csv', help='Path to csv file', nargs='?' ,type=argparse.FileType('r') ,required=True)
    parser_manage_runids.add_argument('-m', '--mode', choices=['link','unlink'] ,required=True)
    parser_manage_runids.set_defaults(func=manage_runids)

    parser_link_run_results = subparser_utilities.add_parser('link_run_results', help='Link the run results for a runID.')
    parser_link_run_results.add_argument('-i', '--runid', help='LIMS runID', default=None)
    # parser_link_run_results.add_argument('-p', '--rundir', help='Path the run directory', required=True)
    parser_link_run_results.set_defaults(func=link_run_results)

    parser_get_researchers = subparser_utilities.add_parser('get_researchers', help='Get all info for all researchers')
    parser_get_researchers.set_defaults(func=get_researchers)

    parser_get_accounts = subparser_utilities.add_parser('get_accounts', help='Get all info for all accounts')
    parser_get_accounts.add_argument('-o','--output_file',  nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='Output file path (default=stdout)')
    parser_get_accounts.set_defaults(func=get_accounts)

    parser_year_overview = subparser_utilities.add_parser('year_overview', help='Create an overview of all USEQ projects in a given year / all years.')
    parser_year_overview.add_argument('-o', '--output', help='Path to output file', nargs='?' ,type=argparse.FileType('w') , default=sys.stdout)
    parser_year_overview.add_argument('-y', '--year', help='Year, leave empty for all', default=None)
    parser_year_overview.set_defaults(func=year_overview)

    parser_route_project = subparser_utilities.add_parser('route_project', help='Routes all samples in project to specific LIMS protocol.')
    parser_route_project.add_argument('-i', '--project_id', help='The LIMS project ID.')
    parser_route_project.add_argument('-p', '--protocol_type',
        help='The LIMS protocol type. Choose from : ISOLATION, LIBPREP, POOLING, POOL QC, ILLUMINA SEQUENCING, NANOPORE SEQUENCING, POST SEQUENCING',
        choices=['ISOLATION', 'LIBPREP', 'POOLING', 'POOL QC', 'ILLUMINA SEQUENCING', 'NANOPORE SEQUENCING', 'POST SEQUENCING']
    )
    parser_route_project.set_defaults(func=route_project)

    parser_sample_report = subparser_utilities.add_parser('sample_report')
    parser_sample_report.add_argument('-i', '--project_id')
    parser_sample_report.set_defaults(func=sample_report)

    parser_finish_run = subparser_utilities.add_parser('finish_run')
    parser_finish_run.add_argument('-i', '--project_id')
    parser_finish_run.add_argument('-f', '--fid')
    parser_finish_run.add_argument('-s', '--succesful', default=True)
    parser_finish_run.set_defaults(func=finish_run)
    # finish_run

    parser_update_stats = subparser_utilities.add_parser('update_stats')
    parser_update_stats.set_defaults(func=update_stats)

    # update_step_udf
    parser_update_step_udf = subparser_utilities.add_parser('update_step_udf')
    parser_update_step_udf.add_argument('-s', '--step', help='Step URI', required=True)
    parser_update_step_udf.add_argument('-u', '--name', help='UDF name', required=True)
    parser_update_step_udf.add_argument('-v', '--value', help='UDF value', required=True)
    parser_update_step_udf.set_defaults(func=update_step_udf)
    #epp parsers
    parser_epp = subparser.add_parser('epp',help='Clarity epp functions: run_status_mail, modify_samplesheet, group_permissions, finance_overview, route_artifacts, close_projects ')
    subparser_epp = parser_epp.add_subparsers()

    parser_run_status_mail = subparser_epp.add_parser('run_status', help='Sends a status email about a run depending on the mode, mail type depends on mode')
    parser_run_status_mail.add_argument('-m', '--mode' ,choices=['run_started','run_finished'])
    parser_run_status_mail.add_argument('-s', '--step_uri', help="The URI of the step that launched this script. Needed for modes: 'run_status', 'run_finished'", default=None)
    parser_run_status_mail.set_defaults(func=run_status_mail)

    parser_modify_samplesheet = subparser_epp.add_parser('modify_samplesheet', help='This script is used to modify a samplesheet to work with either NextSeq or MiSeq/HiSeq. Currently all it does is reverse complement the barcodes when needed')
    parser_modify_samplesheet.add_argument('-s', '--step', help='Step URI', required=True)
    parser_modify_samplesheet.add_argument('-a', '--aid', help='Artifact ID', required=True)
    parser_modify_samplesheet.add_argument('-m', '--mode', help='Run mode', choices=['rev', 'v1tov2'], required=True)
    parser_modify_samplesheet.add_argument('-o','--output_file',  nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='Output file path (default=stdout)')
    parser_modify_samplesheet.set_defaults(func=modify_samplesheet)

    parser_group_permissions = subparser_epp.add_parser('group_permissions', help='Script that checks if a user trying to execute a LIMS step is part of the specified group(s)')
    parser_group_permissions.add_argument('-s', '--step', help='Step URI', required=True)
    parser_group_permissions.add_argument('-g', '--groups', help='Groups to give permission to', required=True)
    parser_group_permissions.set_defaults(func=group_permissions)

    parser_finance_overview = subparser_epp.add_parser('finance_overview', help='Creates a finance overview for all runs included in the step')
    parser_finance_overview.add_argument('-s', '--step', help='Step URI', required=True)
    parser_finance_overview.add_argument('-o','--output_file',  nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='Output file path (default=stdout)')
    parser_finance_overview.set_defaults(func=finance_overview)

    parser_route_artifacts = subparser_epp.add_parser('route_artifacts', help='Route artifacts to the next appropriate step in the workflow')
    parser_route_artifacts.add_argument('-s', '--step', help='Step URI', required=True)
    parser_route_artifacts.add_argument('-i', '--input', help='Use input artifact', default=False)
    parser_route_artifacts.set_defaults(func=route_artifacts)

    parser_close_projects = subparser_epp.add_parser('close_projects', help='Close all projects included in the specified step')
    parser_close_projects.add_argument('-s', '--step', help='Step URI', required=False)
    parser_close_projects.add_argument('-p', '--pid', required=False, default=None, help='ProjectID, Overrides Step URI')
    parser_close_projects.set_defaults(func=close_projects)

    parser_create_recipe = subparser_epp.add_parser('create_recipe', help='Creates a novaseq run recipe. Can only be started from the USEQ - Denature, Dilute and Load (Novaseq) step.')
    parser_create_recipe.add_argument('-s', '--step', help='Step URI', required=True)
    parser_create_recipe.add_argument('-o','--output_file',  nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='Output file path (default=stdout)')
    parser_create_recipe.set_defaults(func=create_recipe)

    parser_create_samplesheet = subparser_epp.add_parser('create_samplesheet', help='Creates a v2 samplesheet.')
    parser_create_samplesheet.add_argument('-s', '--step', help='Step URI', required=True)
    parser_create_samplesheet.add_argument('-o','--output_file',  nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='Output file path (default=stdout)')
    parser_create_samplesheet.add_argument('-t', '--type', help='Sample Sheet type', default=None)
    parser_create_samplesheet.set_defaults(func=create_samplesheet)

    parser_parse_worksheet = subparser_epp.add_parser('parse_worksheet')
    parser_parse_worksheet.add_argument('-s', '--step', help='Step URI', required=True)
    parser_parse_worksheet.add_argument('-a', '--aid', help='Artifact ID', required=True)
    parser_parse_worksheet.add_argument('-o','--output_file',  nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='Output file path (default=stdout)')
    parser_parse_worksheet.add_argument('-m', '--mode', help='Mode, choose illumina or ont. Affects which barcodes are available.', choices=['illumina','ont', 'snp'], required=True)
    parser_parse_worksheet.set_defaults(func=parse_worksheet)

    parser_check_barcodes = subparser_epp.add_parser('check_barcodes')
    parser_check_barcodes.add_argument('-s', '--step', help='Step URI', required=True)
    parser_check_barcodes.set_defaults(func=check_barcodes)

    parser_chromium_addons = subparser_epp.add_parser('chromium_addons')
    parser_chromium_addons.add_argument('-s','--step', help='Step URI', required=True)
    parser_chromium_addons.set_defaults(func=chromium_addons)


    #Daemon parsers
    parser_daemons = subparser.add_parser('daemons', help='USEQ daemon scripts: check_nextcloud_storage,manage_runs ')
    subparser_daemons = parser_daemons.add_subparsers()

    parser_nextcloud_monitor = subparser_daemons.add_parser('nextcloud_monitor', help='Daemon that monitors the NextCloud storage and sends a mail when the threshold has been reached.')
    parser_nextcloud_monitor.set_defaults(func=nextcloud_monitor)

    parser_manage_runs = subparser_daemons.add_parser('manage_runs', help='Daemon responsible for starting conversion, transfer, cleanup and archiving of sequencing runs')
    parser_manage_runs.add_argument('-d', '--skip_demux_check', help='Skip demultiplexing check. Usefull for low quality runs', default=False)
    # parser_manage_runs.add_argument('-b', '--barcode_mismatches', help='Run conversion with n mismatches allowed in index', default=1)
    # parser_manage_runs.add_argument('-f', '--fastq_for_index', help='Create FastQ for index reads', default=False)
    # parser_manage_runs.add_argument('-s', '--short_reads', help='Sets --minimum-trimmed-read-length and --mask-short-adapter-reads to 0 allowing short reads to pass filter', default=False)
    # parser_manage_runs.add_argument('-u', '--use_bases_mask', help='Use this base mask', default=None)
    parser_manage_runs.set_defaults(func=manage_runs)

    parser_run_overview = subparser_daemons.add_parser('run_overview', help='Daemon responsible for updating the run overview json file used in the USEQ-Overview website.')
    parser_run_overview.add_argument('-o', '--overview_file', help='', default='overview.json')
    parser_run_overview.set_defaults(func=run_overview)

    args = parser.parse_args()
    args.func(args)





#EPP Functions
