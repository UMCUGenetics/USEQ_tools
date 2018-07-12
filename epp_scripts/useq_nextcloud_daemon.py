import xml.dom.minidom
from nextcloud_util import NextcloudUtil
from datetime import datetime
from email.mime.text import MIMEText
import smtplib
from useq_nextcloud_daemon_cfg import HOSTNAME,USER, PW,EMAIL,RECIPIENTS

def sendMail(files):

	msg_text = ''

	msg_text += "<h4>The following runs are eligible for removal:</h4>"
	msg_text += "<table>"
	msg_text += "<tr><th>File name</th><th>Upload date</th></tr>"
	for file in files:
		msg_text += "<tr> <td>{0}</td><td>{1}</td> </tr>".format(file.name, file.mtime)
	msg_text += "</table>"

	msg = MIMEText( msg_text.encode('utf-8') , 'html')

	msg[ "Subject" ] = "USEQ NextCloud storage at 90%"
	msg[ "From" ] = EMAIL
	msg[ "To" ] = ";".join( RECIPIENTS )


	s = smtplib.SMTP( "localhost" )
	s.sendmail( EMAIL, RECIPIENTS, msg.as_string() )
	s.quit()


def main():
	global nc_util

	nextcloud_hostname = HOSTNAME
	username = USER
	pw = PW

	nc_util = NextcloudUtil()
	nc_util.setHostname( nextcloud_hostname )
	nc_util.setup( username, pw )

	max_size = 1073741824000 #bytes
	total_size = 0

	files = nc_util.file_overview()
	files_to_delete = []

	for file in files:
		total_size += file.size
		date_diff = datetime.now() - datetime.strptime(file.mtime.split(",")[1], ' %d %b %Y %H:%M:%S GMT')
		if date_diff.days > 1:
			files_to_delete.append(file)

	# print total_size
	usage = (total_size / float(max_size)) * 100
	if usage >= 10:
		sendMail(files_to_delete)

if __name__ == "__main__":

    main()
