"""Module for monitoring and reporting Nextcloud storage usage."""

from typing import Dict, List, Any, TextIO

from config import Config
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_template import render_template
from modules.useq_mail import send_mail
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from genologics.entities import Project
# File size constants
BYTES_PER_KB = 1024.0
SIZE_SUFFIXES = ['B', 'KB', 'MB', 'GB', 'TB']
MAX_SUFFIX_INDEX = 4



def createDBSession():

    #Set up portal db connection +
    Base = automap_base()
    ssl_args = {'ssl_ca': Config.SSL_CERT}
    engine = create_engine(Config.PORTAL_DB_URI, connect_args=ssl_args, pool_pre_ping=True, pool_recycle=21600)

    Base.prepare(engine, reflect=True)
    Run = Base.classes.run
    # IlluminaSequencingStats = Base.classes.illumina_sequencing_stats
    # NanoporeSequencingStats = Base.classes.nanopore_sequencing_stats
    session = Session(engine)

    return (session,Run)


def convert_file_size(size: float, precision: int = 2) -> str:
    """Convert file size in bytes to human-readable format.

    Args:
        size (float): File size in bytes
        precision (int): Number of decimal places to display

    Returns:
        Formatted file size string (e.g., "1.50MB")

    Examples:
        >>> convert_file_size(1024)
        '1.00KB'
        >>> convert_file_size(1536, precision=1)
        '1.5KB'
    """
    suffix_index = 0

    while size > BYTES_PER_KB and suffix_index < MAX_SUFFIX_INDEX:
        suffix_index += 1
        size = size / BYTES_PER_KB

    return f"{size:.{precision}f}{SIZE_SUFFIXES[suffix_index]}"


def _calculate_total_size(files: Dict[str, Dict[str, Any]]) -> int:
    """Calculate total size of all files and convert individual sizes to readable format.

    Args:
        files (Dict[str, Dict[str, Any]]): Dictionary of file information with 'size' in bytes

    Returns:
        Total size in bytes

    Note:
        This function modifies the input dictionary by converting size values
        to human-readable strings.
    """
    total_size = 0

    for file_info in files.values():
        total_size += file_info['size']
        file_info['size'] = convert_file_size(file_info['size'])

    return total_size


def _send_usage_report(nextcloud_util: NextcloudUtil, files: Dict[str, Dict[str, Any]], total_size: int):
    """Send email report with Nextcloud directory usage information.

    Args:
        nextcloud_util (NextcloudUtil): Configured NextcloudUtil instance
        files (Dict[str, Dict[str, Any]]): Dictionary of file information
        total_size (int): Total size in bytes
    """
    usage = convert_file_size(total_size)
    subject = f'Nextcloud overview of directory {nextcloud_util.run_dir}'

    data = {
        'total_usage': usage,
        'files': files,
        'dir': nextcloud_util.run_dir
    }

    content = render_template('nextcloud_overview.html', data)
    # send_mail(subject, content, Config.MAIL_SENDER, Config.MAIL_ADMINS)
    send_mail(subject, content, Config.MAIL_SENDER, 's.w.boymans@umcutrecht.nl')

def _send_reminder_email(lims, files: Dict[str, Dict[str, Any]]):
    """Send reminder email for files that have not been downloaded.

    Args:
        lims: LIMS instance for project information retrieval
        files (Dict[str, Dict[str, Any]]): Dictionary of file information
    """
    # undownloaded_files = {path: info for path, info in files.items() if not info.get('download_count')}
    for path, info in files.items():
        download_count = info.get('download_count', 0)
        expiration = info.get('share_expiration', None)
        if not expiration:
            continue

        expiration_date = datetime.strptime(expiration, "%Y-%m-%d %H:%M:%S")
        days_from_expiration = (expiration_date.date() - datetime.now().date()).days

        if days_from_expiration == Config.NEXTCLOUD_REMINDER and not info.get('download_count'):
            candidate_runid = path.split("/")[-1].split("_")[0]
            print(f"{candidate_runid} is about to expire in {days_from_expiration} days and has not been downloaded yet.")
            project = None
            try:
                project = Project(lims, id=candidate_runid)
            except:
                print(f"Error: Project ID {candidate_runid} not found in LIMS!")
                continue

            researcher = project.researcher
            #print(researcher.first_name, researcher.last_name, researcher.email)
            subject = f"REMINDER: USEQ sequencing of sequencing-run ID {candidate_runid} finished"

            data = {
                'project_id': candidate_runid,
                'name': f"{researcher.first_name} {researcher.last_name}",
                'share_id' :  info.get('share_id'),
                'expiration': expiration_date.strftime("%Y-%m-%d")
            }
            content = render_template('share_reminder_template.html', data)
            send_mail(subject, content, Config.MAIL_SENDER, 's.w.boymans@umcutrecht.nl')

def check_usage(lims, nextcloud_util: NextcloudUtil, historic_shares: Dict[Any, Any], mode: str, download_events: TextIO, download_event_summary: TextIO):
    """Check storage usage for a Nextcloud directory and send report.

    Retrieves file list from Nextcloud, calculates total storage usage,
    and sends an email report to administrators.

    Args:
        lims: LIMS instance for project information retrieval
        nextcloud_util (NextcloudUtil): Configured NextcloudUtil instance with directory set
        historic_shares (Dict[Any, Any]): Dictionary of historic shares
        mode (str): Mode of operation (e.g., 'weekly', 'daily')
        download_events (TextIO): File object for logging download events
        download_event_summary (TextIO): File object for logging download event summaries   
    """


    files = nextcloud_util.file_list(historic_shares)
    total_size = _calculate_total_size(files)
    if mode == 'weekly':
        _send_usage_report(nextcloud_util, files, total_size)
    elif mode == 'daily':
        _send_reminder_email(lims, files)

def _setup_nextcloud_util(directory: str) -> NextcloudUtil:
    """Create and configure a NextcloudUtil instance.

    Args:
        directory (str): Nextcloud directory to monitor

    Returns:
        Configured NextcloudUtil instance
    """
    nextcloud_util = NextcloudUtil()
    nextcloud_util.set_hostname(Config.NEXTCLOUD_HOST)
    nextcloud_util.setup(
        Config.NEXTCLOUD_USER,
        Config.NEXTCLOUD_PW,
        Config.NEXTCLOUD_WEBDAV_ROOT,
        directory,
        Config.MAIL_SENDER
    )

    return nextcloud_util


def run(lims, mode: str = 'weekly', download_events: TextIO = None, download_event_summary: TextIO = None):
    """
    Entry point for Nextcloud usage monitoring.

    Checks storage usage for both raw data and manual directories,
    sending separate reports for each.

    Args:
        lims: LIMS instance for project information retrieval
        mode (str): Mode of operation ('weekly' for usage report, 'daily' for reminder emails)
        download_events (TextIO): File object for logging download events
        download_event_summary (TextIO): File object for logging download event summaries
    """

    session, Run = createDBSession()

    runs_with_share = session.query(Run).filter(
        Run.raw_share.isnot(None),
        Run.raw_share != ''
    ).all()
    historic_shares = {}
    for run in runs_with_share:
        historic_shares[run.run_id] = run.raw_share

    # Check raw directory usage
    nextcloud_util = _setup_nextcloud_util(Config.NEXTCLOUD_RAW_DIR)
    check_usage(lims, nextcloud_util, historic_shares, mode, download_events, download_event_summary)

    # Check manual directory usage
    # nextcloud_util = _setup_nextcloud_util(Config.NEXTCLOUD_MANUAL_DIR)
    # check_usage(lims, nextcloud_util, historic_shares, mode, download_events, download_event_summary)
