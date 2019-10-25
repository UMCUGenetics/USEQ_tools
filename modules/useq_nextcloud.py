import sys
import easywebdav
import os
import ntpath
import requests
import json
from xml.dom.minidom import parseString

DEBUG = 0

class NextcloudUtil(object):

    def __init__( self ):
        if DEBUG > 0: print (self.__module__ + " init called")
        self.hostname = ""
        self.webdav = ""

    def setHostname( self, hostname ):
        if DEBUG > 0: print (self.__module__ + " setHostname called")
        self.hostname = hostname


    def setup( self, user, password, webdav_root,run_dir,recipient  ):

        if DEBUG > 0: print (self.__module__ + " setup called")
        self.user = user
        self.password = password
        self.webdav = easywebdav.connect(self.hostname , username=user, password=password, protocol='https')
        self.webdav_root = webdav_root
        self.run_dir = run_dir
        self.recipient = recipient

    def fileList(self):
        files = {}
        #Check in log files if file was downloaded
        download_ids = {}
        for file in self.webdav.ls(self.webdav_root+"log/"):
            if not file.contenttype: continue #directories
            response = requests.get("https://{0}/{1}".format(self.hostname,file.name),auth=(self.user, self.password))
            for line in response.text.split('\n'):
                if not line.rstrip():continue
                columns = line.split('"')
                if 'download' not in columns[1] :continue
                if not columns[2].startswith(' 200'):continue
                ip = columns[0].split(" ")[0]
                from geoip import geolite2
                ip_match = geolite2.lookup(ip)
                download_date = columns[0].split(" ")[3].lstrip('[')

                download_id = columns[1].split(' ')[1].split('/')[3]
                if download_id not in download_ids:
                    download_ids[download_id] = {'download_sizes':[],'downloaded_from':[], 'download_dates': []}

                download_ids[download_id]['download_sizes'].append(columns[2].split(" ")[2])
                download_ids[download_id]['download_dates'].append(download_date)
                if ip_match:
                    download_ids[download_id]['downloaded_from'].append(ip_match.country)
        # Get a listing of all files
        for file in self.webdav.ls(self.webdav_root+self.run_dir):
            if not file.contenttype: continue #directories
            file_path = file.name.replace(self.webdav_root,'')
            files[file_path] = {
                'size' : file.size,
                'mtime' : file.mtime,
                'share_id' : '',
                'downloaded' : False,
                'download_sizes' : [],
                'downloaded_from' : [],
                'download_dates' : []
            }
        # Get file share ID
        response = requests.get("https://{0}/ocs/v1.php/apps/files_sharing/api/v1/shares".format(self.hostname), auth=(self.user, self.password), headers={'OCS-APIRequest':'true'})
        response_DOM = parseString( response.text )
        for element in response_DOM.getElementsByTagName( "element" ):
            file_path = element.getElementsByTagName("path")[0].firstChild.data
            share_id = element.getElementsByTagName("token")[0].firstChild.data
            if file_path in files:
                files[file_path]['share_id'] = share_id
                if share_id in download_ids:
                    files[file_path]['downloaded'] = True
                    files[file_path]['download_sizes'] = download_ids[share_id]['download_sizes']
                    files[file_path]['downloaded_from'] = download_ids[share_id]['downloaded_from']
                    files[file_path]['download_dates'] = download_ids[share_id]['download_dates']

        return files

    def upload(self, file_path):
        if not os.path.isfile(file_path): return {"ERROR":"File path '{0}' is not a file".format(file_path)}
        file_basename = ntpath.basename(file_path)
        remote_path = self.webdav_root+self.run_dir+file_basename

        if self.webdav.exists(remote_path):

            return {"ERROR" : "File path '{0}' already exists on server".format(file_basename)}
        else:
            #upload file
            self.webdav.upload(file_path, remote_path)

        #check if file upload succeeded
        upload_response = self.webdav.exists(remote_path)
        return {"SUCCES" : upload_response}

    def share(self, file_path, email):
        file_basename = ntpath.basename(file_path)
        remote_path = self.webdav_root+self.run_dir+file_basename

        if not self.webdav.exists(remote_path):
            return {"ERROR" : "File path '{0}' does not exist on server".format(file_basename)}

        data={
            'path' : "{0}/{1}".format(self.run_dir,file_basename),
            'shareType' : 4,
            'shareWith' : 'useq@umcutrecht.nl'
        }

        response = requests.post("https://{0}/ocs/v1.php/apps/files_sharing/api/v1/shares".format(self.hostname), auth=(self.user, self.password), headers={'OCS-APIRequest':'true','Content-Type': 'application/json'},data=json.dumps(data))

        if not response.ok:
            return {"ERROR" : response.raise_for_status()}


        share_id = None
        if not self.webdav.exists(remote_path):
            return {"ERROR" : "File '{0}' upload failed".format(file_basename)}

        response_DOM = parseString( response.text )
        share_id = response_DOM.getElementsByTagName( "token" )[0].firstChild.data
        return {"SUCCES": share_id}
