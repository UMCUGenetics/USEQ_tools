"""Module for interacting with Nextcloud via WebDAV and OCS API."""

import json
import ntpath
import os
import secrets
from pathlib import Path
from typing import Any, Dict, List, Optional
from xml.dom.minidom import parseString

import easywebdav
import requests

# Configuration
DEBUG = 0


class NextcloudUtil:
    """
    Utility class for managing file operations on Nextcloud.

    Provides methods for uploading, sharing, listing, and managing files
    on a Nextcloud server using WebDAV and OCS API.
    """

    def __init__(self):
        """Initialize the NextcloudUtil instance."""
        if DEBUG > 0:
            print(f"{self.__class__.__name__} init called")

        self.hostname = ""
        self.webdav = None
        self.user = ""
        self.password = ""
        self.webdav_root = ""
        self.run_dir = ""
        self.recipient = ""

    def set_hostname(self, hostname: str):
        """
        Set the Nextcloud server hostname.

        Args:
            hostname (str): The hostname of the Nextcloud server.
        """
        if DEBUG > 0:
            print(f"{self.__class__.__name__} set_hostname called")

        self.hostname = hostname

    def setup(self, user: str, password: str, webdav_root: str, run_dir: str, recipient: str):
        """
        Configure the Nextcloud connection and paths.

        Args:
            user (str): Nextcloud username.
            password (str): Nextcloud password.
            webdav_root (str): Root path for WebDAV operations.
            run_dir (str): Directory for the current run.
            recipient (str): Email recipient for shares.
        """
        if DEBUG > 0:
            print(f"{self.__class__.__name__} setup called")

        self.user = user
        self.password = password
        self.webdav = easywebdav.connect(self.hostname, username=user, password=password, protocol="https")
        self.webdav_root = webdav_root
        self.run_dir = run_dir
        self.recipient = recipient

    def simple_file_list(self, directory: str) -> List[str]:
        """
        Get a simple list of filenames in a directory.

        Args:
            directory (str): Directory path relative to webdav_root/run_dir.

        Returns:
            List of filenames (excluding directories).
        """
        files = []
        path = f"{self.webdav_root}{self.run_dir}/{directory}"

        for file in self.webdav.ls(path):
            if not file.contenttype:  # Skip directories
                continue

            file_path = file.name.replace(self.webdav_root, "")
            file_name = file_path.split("/")[-1]
            files.append(file_name)

        return files

    def file_list(self) -> Dict[str, Dict[str, Any]]:
        """
        Get detailed information about files including download statistics.

        Returns:
            Dictionary mapping file paths to file metadata including:
            - size: File size in bytes
            - mtime: Modification time
            - share_id: Share token if file is shared
            - downloaded: Whether file has been downloaded
            - download_sizes: List of download sizes from logs
            - downloaded_from: List of countries where downloaded from
            - download_dates: List of download timestamps
        """
        files = {}
        download_ids = {}

        # Parse download logs
        download_ids = self._parse_download_logs()

        # List files in run directory
        for file in self.webdav.ls(f"{self.webdav_root}{self.run_dir}"):
            file_path = file.name.replace(self.webdav_root, "")

            # Skip certain files and directories
            if (file_path.endswith(".done") or
                file_path.endswith("raw_data/") or
                file_path.endswith("other_data/")):
                continue

            size = 0
            if file.contenttype:
                size = file.size
            else:  # Directory - sum size of all files
                for subfile in self.webdav.ls(f"{self.webdav_root}{file_path}"):
                    size += subfile.size
                file_path = file_path[:-1]  # Remove trailing slash

            files[file_path] = {
                "size": size,
                "mtime": file.mtime,
                "share_id": "",
                "downloaded": False,
                "download_sizes": [],
                "downloaded_from": [],
                "download_dates": [],
            }

        # Get share IDs and match with download logs
        self._populate_share_info(files, download_ids)

        return files

    def _parse_download_logs(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Parse download logs to extract download statistics.

        Returns:
            Dictionary mapping share IDs to download information.
        """
        download_ids = {}

        for file in self.webdav.ls(f"{self.webdav_root}log/"):
            if not file.contenttype:  # Skip directories
                continue

            url = f"https://{self.hostname}/{file.name}"
            response = requests.get(url, auth=(self.user, self.password))

            if DEBUG > 0:
                print(file.name, file.size)

            for line in response.text.split("\n"):
                if not line.rstrip():
                    continue

                columns = line.split('"')
                if not columns[2].startswith(" 200"):
                    continue

                # Extract IP and geolocate
                ip = columns[0].split(" ")[0]
                ip_match = self._geolocate_ip(ip)

                # Extract download information
                download_date = columns[0].split(" ")[3].lstrip("[")
                download_id_fields = columns[1].split(" ")[1].split("/")

                # Determine download ID from URL structure
                if download_id_fields[2] == "s":
                    download_id = download_id_fields[3]
                elif download_id_fields[1] == "s":
                    download_id = download_id_fields[2]
                else:
                    download_id = columns[0].split(" ")[2]

                # Initialize download tracking for this ID
                if download_id not in download_ids:
                    download_ids[download_id] = {
                        "download_sizes": [],
                        "downloaded_from": [],
                        "download_dates": [],
                    }

                # Record download information
                download_ids[download_id]["download_sizes"].append(
                    columns[2].split(" ")[2]
                )
                download_ids[download_id]["download_dates"].append(download_date)

                if ip_match:
                    download_ids[download_id]["downloaded_from"].append(
                        ip_match.country
                    )

        return download_ids

    def _geolocate_ip(self, ip: str) -> Optional[Any]:
        """
        Geolocate an IP address.

        Args:
            ip (str): IP address to geolocate.

        Returns:
            Geolocation match object or None if geolocation fails.
        """
        try:
            from geoip import geolite2
            return geolite2.lookup(ip)
        except Exception:
            return None

    def _populate_share_info(self, files: Dict[str, Dict[str, Any]], download_ids: Dict[str, Dict[str, List[str]]]):
        """
        Populate share IDs and download information for files.

        Args:
            files (Dict[str, Dict[str, Any]]): Dictionary of file information to update.
            download_ids (Dict[str, Dict[str, List[str]]]): Dictionary of download statistics by share ID.
        """
        url = f"https://{self.hostname}/ocs/v2.php/apps/files_sharing/api/v1/shares"
        response = requests.get(
            url,
            auth=(self.user, self.password),
            headers={"OCS-APIRequest": "true"},
        )

        response_dom = parseString(response.text)

        for element in response_dom.getElementsByTagName("element"):
            file_path = element.getElementsByTagName("path")[0].firstChild.data
            share_id = element.getElementsByTagName("token")[0].firstChild.data

            if file_path in files:
                files[file_path]["share_id"] = share_id

                if share_id in download_ids:
                    files[file_path]["downloaded"] = True
                    files[file_path]["download_sizes"] = download_ids[share_id]["download_sizes"]
                    files[file_path]["downloaded_from"] = download_ids[share_id]["downloaded_from"]
                    files[file_path]["download_dates"] = download_ids[share_id]["download_dates"]

    def check_exists(self, file: str) -> bool:
        """
        Check if a file exists on the server.

        Args:
            file (str): Filename relative to webdav_root/run_dir.

        Returns:
            True if file exists, False otherwise.
        """
        remote_path = f"{self.webdav_root}/{self.run_dir}/{file}"
        return self.webdav.exists(remote_path)

    def delete(self, file: str):
        """
        Delete a file from the server.

        Args:
            file (str): Filename relative to webdav_root/run_dir.
        """
        remote_path = f"{self.webdav_root}/{self.run_dir}/{file}"
        self.webdav.delete(remote_path)

    def create_dir(self, directory: str):
        """
        Create a directory on the server.

        Args:
            directory: Directory name relative to webdav_root/run_dir.
        """
        remote_path = f"{self.webdav_root}/{self.run_dir}/{directory}"
        self.webdav.mkdir(remote_path)

    def upload(self, file_path: str) -> Dict[str, Any]:
        """
        Upload a file to the server.

        Args:
            file_path (str): Local path to the file to upload.

        Returns:
            Dictionary with either "SUCCESS" (bool) or "ERROR" (str) key.
        """
        if not os.path.isfile(file_path):
            return {"ERROR": f"File path '{file_path}' is not a file"}

        file_basename = ntpath.basename(file_path)
        remote_path = f"{self.webdav_root}{self.run_dir}{file_basename}"

        if self.webdav.exists(remote_path):
            return {"ERROR": f"File path '{file_basename}' already exists on server"}

        # Upload file
        self.webdav.upload(file_path, remote_path)

        # Verify upload
        upload_success = self.webdav.exists(remote_path)
        return {"SUCCESS": upload_success}

    def share(self, file_name: str, email: str) -> Dict[str, Any]:
        """
        Create a password-protected share link for a file.

        Args:
            file_name (str): Name of the file to share.
            email (str): Email address to associate with the share (currently unused).

        Returns:
            Dictionary with either "SUCCESS" ([share_id, password]) or "ERROR" (str) key.
        """

        remote_path = f"{self.webdav_root}/{self.run_dir}/{file_name}"

        if not self.webdav.exists(remote_path):
            return {"ERROR": f"File path '{file_name}' does not exist on server"}

        # Generate secure random password
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
        password = "".join(secrets.choice(alphabet) for _ in range(12))

        # Create share via OCS API
        data = {
            "path": f"{self.run_dir}/{file_name}",
            "shareType": 3,
            "shareWith": "useq@umcutrecht.nl",
            "password": password,
        }

        url = f"https://{self.hostname}/ocs/v2.php/apps/files_sharing/api/v1/shares"
        response = requests.post(
            url,
            auth=(self.user, self.password),
            headers={"OCS-APIRequest": "true", "Content-Type": "application/json"},
            data=json.dumps(data),
        )

        if not response.ok:
            return {"ERROR": str(response.status_code)}

        # Extract share ID from response
        response_dom = parseString(response.text)
        share_id = response_dom.getElementsByTagName("token")[0].firstChild.data
        
        return {"SUCCESS": [share_id, password]}
