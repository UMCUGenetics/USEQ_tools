from config import NEXTCLOUD_HOST,NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,NEXTCLOUD_PROCESSED_DIR,NEXTCLOUD_LOG_DIR,MAIL_SENDER, NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_STORAGE, NEXTCLOUD_MAX,MAIL_SENDER,MAIL_ADMINS,NEXTCLOUD_MANUAL_DIR
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from modules.useq_mail import sendMail
from datetime import datetime

def convertFileSize(size,precision=2):
    suffixes=['B','KB','MB','GB','TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1 #increment the index of the suffix
        size = size/1024.0 #apply the division
    return "%.*f%s"%(precision,size,suffixes[suffixIndex])


def checkUsage():
	files = nextcloud_util.fileList()
	files_to_delete =[]
	total_size = 0

	for file in files:
		total_size += files[file]['size']
		files[file]['size'] = convertFileSize(files[file]['size'])

	usage = convertFileSize(total_size)
	subject = 'Nextcloud overview of directory {0}'.format(nextcloud_util.run_dir)
	data = {
		'total_usage' : usage,
		'files' : files,
		'dir' : nextcloud_util.run_dir
	}

	content = renderTemplate('nextcloud_overview.html', data)
	# sendMail(subject,content,MAIL_SENDER,MAIL_ADMINS)

def run():
    global nextcloud_util
    #Set up nextcloud
    nextcloud_util = NextcloudUtil()
    nextcloud_util.setHostname( NEXTCLOUD_HOST )

    nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,MAIL_SENDER )
    checkUsage()
    # #
    # # # nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_PROCESSED_DIR,MAIL_SENDER )
    # # # checkUsage()
    # #
    # nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_MANUAL_DIR,MAIL_SENDER )
    # checkUsage()
