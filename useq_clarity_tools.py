#!/env/bin/python
"""USEQ Clarity tools"""

import sys
import argparse


from genologics.lims import Lims

# import resources
import utilities
import epp

import config


#Commandline utility Functions
def manage_accounts(args):
    """Create,Edit,Retrieve accounts (labs)"""
    utilities.useq_manage_accounts.run(lims, args.mode, args.csv, args.account)

def client_mail(args):
    """Send email to all specific USEQ clients, all clients belonging to an account or a single specific client."""
    utilities.useq_client_mail.run(lims, config.MAIL_SENDER, args.content, args.mode, args.attachment, args.name)

def share_run(args):
    """Encrypt and Share one or more sequencing runs"""
    utilities.useq_share_run.run(lims, args.ids)


#Clarity epp scripts
def run_status_mail(args):
    """Send run started mail"""
    epp.useq_run_status_mail.run(lims, config.MAIL_SENDER, config.MAIL_ADMINS, args.mode ,args.step_uri)

if __name__ == "__main__":
    global lims

    # Setup lims connection
    lims = Lims(config.BASEURI, config.USERNAME, config.PASSWORD)

    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()

    # output_parser = argparse.ArgumentParser(add_help=False)
    # output_parser.add_argument('-o', '--output_file', nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='Output file path (default=stdout)')


    #Utility parsers
    parser_utilities = subparser.add_parser('utilities',help="Utility functions: manage_accounts, client_mail")
    subparser_utilities = parser_utilities.add_subparsers()

    parser_manage_accounts = subparser_utilities.add_parser('manage_accounts', help='Create, Edit & Retrieve accounts (labs)')
    parser_manage_accounts.add_argument('-m','--mode',choices=['create','edit','retrieve'])
    parser_manage_accounts.add_argument('-c','--csv', help='Path to input or output csv file')
    parser_manage_accounts.add_argument('-a','--account', help='Account name or ID. Leave empty for mode "create"', default=None)
    parser_manage_accounts.set_defaults(func=manage_accounts)

    parser_client_mail = subparser_utilities.add_parser('client_mail', help='Send email to all specific USEQ users, all clients belonging to an account or a single specific client.')
    parser_client_mail.add_argument('-m','--mode',choices=['all','labs','accounts'])
    parser_client_mail.add_argument('-c','--content', help='Path to content file (see resources for example)', nargs='?' ,type=argparse.FileType('r'))
    parser_client_mail.add_argument('-n','--name', help='Lab or Account name(s) separated by comma. Leave empty for mode "all"')
    parser_client_mail.add_argument('-a','--attachment', help='Path to attachment file')
    parser_client_mail.set_defaults(func=client_mail)

    parser_share_run = subparser_utilities.add_parser('share_run', help='Encrypts and shares 1 or more sequencing runs to the appropriate client')
    parser_share_run.add_argument('-i', '--ids', help='One or more Project ID(s) to share, separated by comma')
    parser_share_run.set_defaults(func=share_run)

    #epp parsers
    parser_epp = subparser.add_parser('epp',help='Clarity epp functions: run_started,')
    subparser_epp = parser_epp.add_subparsers()

    parser_run_status_mail = subparser_epp.add_parser('run_status', help='Sends a status email about a run depending on the mode, mail type depends on mode')
    parser_run_status_mail.add_argument('-m', '--mode' ,choices=['run_started'])
    parser_run_status_mail.add_argument('-s', '--step_uri', help="The URI of the step that launched this script. Needed for modes: 'run_status'", default=None)
    parser_run_status_mail.set_defaults(func=run_status_mail)

    args = parser.parse_args()
    # print args
    args.func(args)





#EPP Functions
