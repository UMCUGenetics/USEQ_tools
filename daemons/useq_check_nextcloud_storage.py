from config import NEXTCLOUD_HOST,NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,NEXTCLOUD_PROCESSED_DIR,MAIL_SENDER, NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_STORAGE, NEXTCLOUD_MAX,MAIL_SENDER,MAIL_ADMINS
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from modules.useq_mail import sendMail
from datetime import datetime

def checkUsage():
	files = nextcloud_util.fileList()
	files_to_delete =[]
	total_size = 0

	for file in files:
		total_size += file.size
		date_diff = datetime.now() - datetime.strptime(file.mtime.split(",")[1], ' %d %b %Y %H:%M:%S GMT')
		if date_diff.days > 14:
			files_to_delete.append(file)


	usage = (total_size / float(NEXTCLOUD_STORAGE)) * 100
	if usage >= NEXTCLOUD_MAX:
		subject = 'NextCloud host {0} directory {1} storage at {2}%!'.format(NEXTCLOUD_HOST, nextcloud_util.run_dir,NEXTCLOUD_MAX)
		content = renderTemplate('nextcloud_to_delete.html', {'files':files_to_delete})
		sendMail(subject,content,MAIL_SENDER,MAIL_ADMINS)

def run():
	global nextcloud_util
	#Set up nextcloud
	nextcloud_util = NextcloudUtil()
	nextcloud_util.setHostname( NEXTCLOUD_HOST )

	nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_RAW_DIR,MAIL_SENDER )
	checkUsage()

	nextcloud_util.setup( NEXTCLOUD_USER, NEXTCLOUD_PW, NEXTCLOUD_WEBDAV_ROOT,NEXTCLOUD_PROCESSED_DIR,MAIL_SENDER )
	checkUsage()
