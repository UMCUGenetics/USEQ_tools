"""Module for interacting with Nextcloud via WebDAV and OCS API."""

import re
import sys
import time
import json
import ntpath
import os
import secrets
import csv
import ipaddress
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO
from xml.dom.minidom import parseString
from collections import defaultdict
from urllib.parse import urlparse, parse_qs, unquote
from config import Config
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

    def _check_download_event_integrity(self, events):
        """
        Compare repeated download requests for the same (token, filename).
        For a single named file, its true size cannot legitimately change between
        requests -- if it does, the smaller transfer(s) are flagged as likely
        incomplete/failed, and the largest is flagged as the likely successful one.
        For whole-share/root downloads (filename is None), size differences are
        ambiguous (folder contents may have changed between requests), so these
        are only flagged as "ambiguous", not failed.
        """
        groups = defaultdict(list)
        downloads = [e for e in events if e['action'] == 'download']
        for e in downloads:
            groups[(e['token'], e['filename'])].append(e)

        # default: every download is assumed successful (200 + bytes sent)
        for e in downloads:
            e['likely_status'] = 'success'

        flagged = []
        for (token, filename), evs in groups.items():
            if len(evs) < 2:
                continue
            sizes = set(e['size'] for e in evs)
            if len(sizes) <= 1:
                continue  # identical repeats -- not a failure signal

            if filename is None:
                # whole-share/root download: differing sizes could just mean
                # folder contents changed between requests
                for e in evs:
                    e['likely_status'] = 'ambiguous (whole-share size differs between requests; folder contents may have changed)'
                flagged.append((token, filename, evs, 'ambiguous'))
            else:
                max_size = max(int(e['size']) for e in evs)
                for e in evs:
                    if int(e['size']) < max_size:
                        e['likely_status'] = 'likely incomplete/failed (smaller than a later transfer of the same file)'
                    else:
                        e['likely_status'] = 'likely successful (largest transfer of this file)'
                flagged.append((token, filename, evs, 'incomplete_then_retried'))

        return flagged

    def _summarize_download_events(self, events): #Needs DOC
        by_token = defaultdict(list)
        for e in events:
            by_token[e['token']].append(e)

        # for token, evs in by_token.items():
        #     downloads = [e for e in evs if e['action'] in ('download', 'download_attempt')]
        #     views = [e for e in evs if e['action'] == 'view']
        #     not_found = [e for e in evs if e['action'] == 'not_found']

            # print(f"\n=== Share token: {token} ===")
            # if downloads:
            #     print(f"  DOWNLOADED: yes ({len(downloads)} download request(s))")
            # else:
            #     print("  DOWNLOADED: no confirmed download requests found")

            # if views:
            #     ips_viewed = sorted(set(e['ip'] for e in views))
            #     print(f"  Viewed (link opened) {len(views)}x by IP(s): {', '.join(ips_viewed)}")

            # if not_found:
            #     ips_404 = sorted(set(e['ip'] for e in not_found))
            #     print(f"  404 / invalid-link hits: {len(not_found)}x from {', '.join(ips_404)}")

            # for e in downloads:
            #     fname = e['filename'] or '(root/whole share)'
            #     print(f"    -> {e['time']}  ip={e['ip']:<16} file={fname}  status={e['status']}  size={e['size']}")

        return by_token
    def _format_location(self, geo):
        if not geo:
            return 'unknown'
        parts = [p for p in (geo.get('city'), geo.get('region'), geo.get('country')) if p]
        loc = ', '.join(parts) if parts else (geo.get('country') or 'unknown')
        if geo.get('isp'):
            loc += f" ({geo['isp']})"
        return loc

    def _build_token_summary(self, events, geo):
        by_token = defaultdict(list)
        for e in events:
            by_token[e['token']].append(e)

        summary = {}
        for token, evs in sorted(by_token.items()):
            downloads = [e for e in evs if e['action'] == 'download']
            views = [e for e in evs if e['action'] == 'view']
            not_found = [e for e in evs if e['action'] == 'not_found']
            pw_prompts = [e for e in evs if e['action'] == 'password_prompt']

            ips = sorted(set(e['ip'] for e in evs))
            ip_locations = '; '.join(f"{ip} [{self._format_location(geo.get(ip))}]" for ip in ips)

            filenames = sorted(set(e['filename'] for e in downloads if e['filename']))
            total_bytes = sum(int(e['size']) for e in downloads if e['size'].isdigit())

            times = sorted(e['time'] for e in evs)
            integrity_issues = sorted(set(
                e['likely_status'] for e in downloads
                if e.get('likely_status') and e['likely_status'] != 'success'
            ))
    
            summary[token] = {
                'token': token,
                'downloaded': 'yes' if downloads else 'no',
                'download_count': len(downloads),
                'view_count': len(views),
                'password_prompt_count': len(pw_prompts),
                'not_found_count': len(not_found),
                'unique_ips': len(ips),
                'ip_locations': ip_locations,
                'files': '; '.join(filenames) if filenames else '',
                'total_bytes_downloaded': total_bytes,
                'first_seen': times[0] if times else '',
                'last_seen': times[-1] if times else '',
                'integrity_flags': '; '.join(integrity_issues) if integrity_issues else 'OK',
            }
        return summary

    def _write_file_summary_csv(self, summary, delimiter=';'):
        """Write a summary of file information to a CSV file."""
        
        fields =[
            'file', 'size', 'mtime', 'share_expiration', 'share_id', 'downloaded', 'download_count', 'view_count', 
            'not_found_count', 'unique_ips', 'ip_locations', 'files', 'total_bytes_downloaded', 'first_seen', 'last_seen', 'integrity_flags'
        ]
        with open(Config.NEXTCLOUD_DOWNLOAD_SUMMARY, 'w', newline='') as out_path:
            w = csv.DictWriter(out_path, fieldnames=fields, delimiter=delimiter)
            w.writeheader()
            w.writerows(summary.values())


    def file_list(self, historic_shares: Dict[Any,Any]) -> Dict[str, Dict[str, Any]]:
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
        # download_ids = {}

        # Parse download logs
        events = self._parse_download_logs()

        self._summarize_download_events(events)

        flagged = self._check_download_event_integrity(events)

        all_ips = sorted(set(e['ip'] for e in events ))

        geo = self._geolocate_ips(all_ips)

        token_summary = self._build_token_summary(events, geo)
     
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
                "file": file_path,
                "size": size,
                "mtime": file.mtime,
                "share_expiration" : "",
                "share_id": "",
                "downloaded": "",
                "download_count": 0,
                "view_count": 0,
                "not_found_count": 0,
                "unique_ips": 0,
                "ip_locations": "",
                "files": "",
                "total_bytes_downloaded": 0,
                "first_seen": "",
                "last_seen": "",
                "integrity_flags": "",
            }

        # Get share IDs and match with download logs
        self._populate_share_info(files, token_summary, historic_shares)

        self._write_file_summary_csv(files)

        return files

    def _classify_log_line(self, path, status, user=None):
        """Return (token, action, filename). action is one of:
        'download', 'view', 'password_prompt', 'not_found', 'other'.
        """
        # WebDAV public-share access: /public.php/webdav/<optional file path>
        WEBDAV_RE = re.compile(r'^/public\.php/webdav(?P<rest>/.*)?$')

        # Matches both /index.php/s/TOKEN and /s/TOKEN, optionally with a subpath
        SHARE_RE = re.compile(r'^/(?:index\.php/)?s/(?P<token>[^/?]+)(?P<rest>/.*)?$')

        # Reserved Nextcloud paths that look like a share token but aren't one
        RESERVED_TOKENS = {'login'}

        parsed = urlparse(path)
        qs = parse_qs(parsed.query)

        # --- WebDAV public-share access: token comes from the auth-user field ---
        wd = WEBDAV_RE.match(parsed.path)
        if wd:
            if not user or user == '-':
                return None, None, None  # no token available, can't attribute this request
            rest = (wd.group('rest') or '').lstrip('/')
            filename = unquote(qs['files'][0]) if 'files' in qs else (unquote(rest) if rest else None)
            action = 'download' if status in ('200', '206', '304') else 'download_attempt'
            return user, action, filename

        # --- Standard /s/TOKEN or /index.php/s/TOKEN access ---
        m = SHARE_RE.match(parsed.path)
        if not m:
            return None, None, None

        token = m.group('token')
        if token in RESERVED_TOKENS:
            return None, None, None  # e.g. /index.php/s/login -- not a real share

        rest = m.group('rest') or ''

        filename = None
        if 'files' in qs:
            filename = unquote(qs['files'][0])
        elif rest.startswith('/download/'):
            filename = unquote(rest[len('/download/'):])

        is_download_path = rest.startswith('/download')
        is_auth_path = rest.startswith('/authenticate')

        if status == '404':
            action = 'not_found'
        elif is_download_path and status in ('200', '304', '206'):
            action = 'download'
        elif is_download_path:
            action = 'download_attempt'  # hit /download but got a non-200 status
        elif is_auth_path:
            action = 'password_prompt'  # password-protected share: password page/submission, not a download
        elif rest == '' and status in ('200', '303'):
            action = 'view'  # share landing page, either redirected (303) or rendered directly (200)
        else:
            action = 'other'

        return token, action, filename

    def _parse_download_logs(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Parse download logs to extract download statistics.

        Returns:
            Dictionary mapping share IDs to download information.
        """
        # Combined log format:
        # IP - - [05/Jan/2026:09:51:46 +0100] "GET /index.php/s/TOKEN HTTP/2.0" 303 0 "-" "UA string"
        # For WebDAV requests, the share token shows up in the auth-user (3rd) field instead of "-":
        # IP - TOKEN [20/Jul/2026:11:05:45 +0200] "GET /public.php/webdav/file.csv HTTP/2.0" 200 326 "-" "curl/7.61.1"
        LOG_RE = re.compile(
            r'(?P<ip>\S+)\s+\S+\s+(?P<user>\S+)\s+'
            r'\[(?P<time>[^\]]+)\]\s+'
            r'"(?P<method>\S+)\s+(?P<path>\S+)\s+(?P<proto>[^"]+)"\s+'
            r'(?P<status>\d{3})\s+(?P<size>\S+)\s+'
            r'"(?P<referrer>[^"]*)"\s+"(?P<agent>[^"]*)"'
        )

        # Some logs are the result of grepping across multiple rotated/gzipped log files,
        # which glues a "filename:" prefix onto the front of each line, e.g.:
        #   ssl-nextcloud_server_main.access.log-20260721.gz:10.132.252.91 - - [...] ...
        # Strip that prefix (only) when what follows looks like a real IPv4 address.
        LOG_PREFIX_RE = re.compile(r'^\S*?:(?=\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\s)')

        events = []

        for file in self.webdav.ls(f"{self.webdav_root}log/"):
            if not file.contenttype:  # Skip directories
                continue

            url = f"https://{self.hostname}/{file.name}"
            response = requests.get(url, auth=(self.user, self.password))
            response.raise_for_status()
            if DEBUG > 0:
                print(file.name, file.size)

            for lineno, line in enumerate(response.iter_lines(decode_unicode=True), 1):
                line = line.strip()
                if not line:
                    continue
                line = LOG_PREFIX_RE.sub('', line)  # strip any "somefile.gz:" prefix from grepped logs
                m = LOG_RE.match(line)
                if not m:
                    print(f"WARNING: could not parse line {lineno}: {line[:120]}", file=sys.stderr)
                    continue
                d = m.groupdict()
                token, action, filename = self._classify_log_line(d['path'], d['status'], user=d.get('user'))
                if token is None:
                    continue  # not a share-related request
                events.append({
                    'token': token,
                    'action': action,
                    'filename': filename,
                    'ip': d['ip'],
                    'time': d['time'],
                    'status': d['status'],
                    'size': d['size'],
                    'agent': d['agent'],
                    'raw_path': d['path'],
                })

        return events

    def _geolocate_ips(self, ips: List) -> Dict[Any, Any]:
        """
        Resolve each IP to a rough location using ip-api.com's free batch endpoint
        (no API key needed, up to 100 IPs per request, ~45 req/min rate limit).
        Private/reserved IPs are detected locally without any network call.
        Requires the 'requests' package (pip install requests --break-system-packages).
        Returns {ip: {'country', 'region', 'city', 'isp', 'org', 'as'}}.
        """
        results = {}
        to_query = []

        for ip in ips:
            try:
                addr = ipaddress.ip_address(ip)
            except ValueError:
                results[ip] = {'country': 'invalid IP', 'region': '', 'city': '', 'isp': '', 'org': '', 'as': ''}
                continue
            if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved or addr.is_multicast:
                results[ip] = {'country': 'Private/Internal network', 'region': '', 'city': '', 'isp': '', 'org': '', 'as': ''}
                continue
            to_query.append(ip)

        if not to_query:
            return results

        url = 'http://ip-api.com/batch?fields=status,message,country,regionName,city,isp,org,as,query'
        for i in range(0, len(to_query), 100):
            batch = to_query[i:i + 100]
            try:
                resp = requests.post(url, json=batch, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                for entry in data:
                    ip = entry.get('query')
                    if entry.get('status') == 'success':
                        results[ip] = {
                            'country': entry.get('country', ''),
                            'region': entry.get('regionName', ''),
                            'city': entry.get('city', ''),
                            'isp': entry.get('isp', ''),
                            'org': entry.get('org', ''),
                            'as': entry.get('as', ''),
                        }
                    else:
                        results[ip] = {'country': f"lookup failed ({entry.get('message', 'unknown')})", 'region': '', 'city': '', 'isp': '', 'org': '', 'as': ''}
            except Exception as e:
                for ip in batch:
                    results[ip] = {'country': f'lookup unavailable ({e.__class__.__name__})', 'region': '', 'city': '', 'isp': '', 'org': '', 'as': ''}
            if i + 100 < len(to_query):
                time.sleep(1.5)  # stay well under the free-tier rate limit

        return results

    def _populate_share_info(self, files: Dict[str, Dict[str, Any]], download_ids: Dict[str, Dict[str, List[str]]], historic_shares: Dict[Any, Any]):
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
        active_shares = {}

        for element in response_dom.getElementsByTagName("element"):
            file_path = element.getElementsByTagName("path")[0].firstChild.data
            
            active_shares[file_path] = {
                'share_id' : element.getElementsByTagName("token")[0].firstChild.data,
                'expiration_date' : element.getElementsByTagName("expiration")[0].firstChild.data
            }

        for file_path in files:

            if file_path in active_shares:
                share_id = active_shares[file_path]['share_id']
                files[file_path]["share_id"] = share_id
                files[file_path]["share_expiration"] = active_shares[file_path]['expiration_date']

                if share_id in download_ids:
                    files[file_path]["downloaded"] = download_ids[share_id]["downloaded"]
                    files[file_path]["download_count"] = download_ids[share_id]["download_count"]
                    files[file_path]["view_count"] = download_ids[share_id]["view_count"]
                    files[file_path]["not_found_count"] = download_ids[share_id]["not_found_count"]
                    files[file_path]["unique_ips"] = download_ids[share_id]["unique_ips"]
                    files[file_path]["ip_locations"] = download_ids[share_id]["ip_locations"]
                    files[file_path]["files"] = download_ids[share_id]["files"]
                    files[file_path]["total_bytes_downloaded"] = download_ids[share_id]["total_bytes_downloaded"]
                    files[file_path]["first_seen"] = download_ids[share_id]["first_seen"]
                    files[file_path]["last_seen"] = download_ids[share_id]["last_seen"]
                    files[file_path]["integrity_flags"] = download_ids[share_id]["integrity_flags"]

            else:
                candidate_runid = file_path.split("/")[-1].split("_")[0]
                
                if candidate_runid in historic_shares:
                    share_id = historic_shares[candidate_runid]

                    files[file_path]["share_id"] = share_id
                    
                    files[file_path]["share_expiration"] = None

                    if share_id in download_ids:
                        files[file_path]["downloaded"] = download_ids[share_id]["downloaded"]
                        files[file_path]["download_count"] = download_ids[share_id]["download_count"]
                        files[file_path]["view_count"] = download_ids[share_id]["view_count"]
                        files[file_path]["not_found_count"] = download_ids[share_id]["not_found_count"]
                        files[file_path]["unique_ips"] = download_ids[share_id]["unique_ips"]
                        files[file_path]["ip_locations"] = download_ids[share_id]["ip_locations"]
                        files[file_path]["files"] = download_ids[share_id]["files"]
                        files[file_path]["total_bytes_downloaded"] = download_ids[share_id]["total_bytes_downloaded"]
                        files[file_path]["first_seen"] = download_ids[share_id]["first_seen"]
                        files[file_path]["last_seen"] = download_ids[share_id]["last_seen"]
                        files[file_path]["integrity_flags"] = download_ids[share_id]["integrity_flags"]



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
