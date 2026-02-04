"""Module for monitoring and reporting Nextcloud storage usage."""

from typing import Dict, List, Any

from config import Config
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_template import render_template
from modules.useq_mail import send_mail


# File size constants
BYTES_PER_KB = 1024.0
SIZE_SUFFIXES = ['B', 'KB', 'MB', 'GB', 'TB']
MAX_SUFFIX_INDEX = 4


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
    send_mail(subject, content, Config.MAIL_SENDER, Config.MAIL_ADMINS)


def check_usage(nextcloud_util: NextcloudUtil):
    """Check storage usage for a Nextcloud directory and send report.

    Retrieves file list from Nextcloud, calculates total storage usage,
    and sends an email report to administrators.

    Args:
        nextcloud_util (NextcloudUtil): Configured NextcloudUtil instance with directory set
    """
    files = nextcloud_util.file_list()
    total_size = _calculate_total_size(files)
    _send_usage_report(nextcloud_util, files, total_size)


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


def run():
    """
    Entry point for Nextcloud usage monitoring.

    Checks storage usage for both raw data and manual directories,
    sending separate reports for each.
    """
    # Check raw directory usage
    nextcloud_util = _setup_nextcloud_util(Config.NEXTCLOUD_RAW_DIR)
    check_usage(nextcloud_util)

    # Check manual directory usage
    nextcloud_util = _setup_nextcloud_util(Config.NEXTCLOUD_MANUAL_DIR)
    check_usage(nextcloud_util)
