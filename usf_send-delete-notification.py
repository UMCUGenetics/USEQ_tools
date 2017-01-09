import smtplib

from optparse import OptionParser
from email.mime.text import MIMEText

options = None




def sendMails ( mailInfo ):
    
    for email in mailInfo:
	TEXT = []
	TEXT.append( "<p>Dear USF customer,</p>")
	TEXT.append( "<p>The following runs are scheduled for deletion from our servers:</p>" )
	TEXT.append( "<table><thead><tr> <th><b>Flowcell</b></th> <th><b>LIMS Name</b></th> <th><b>LIMS ID</b></th> </tr>" )
	
	for run in mailInfo[ email ]:
	    TEXT.append( "<tr> <td>%s</td> <td>%s</td> <td>%s</td> </tr>" % ( run['Flowcell'],run['LIMS Name'],run['LIMS ID'] ) )
	
	TEXT.append( "</thead></table>" )
	TEXT.append( "<p>Make sure you have your data backed up somewhere safe. If we don't hear back from you within 7 days we'll delete the data without further notice.</p>")
	TEXT.append( "<p>Kind regards,</p>")
	TEXT.append( "<p>Sander Boymans</p>")
    
	message = MIMEText( "\r\n".join( TEXT ), 'html' )

	message[ "Subject" ] = 'USF - data scheduled for deletion'
	message[ "From" ] = options.mail
	message[ "To"] = email
	
	s = smtplib.SMTP( "localhost" )
	s.sendmail( options.mail, email, message.as_string() )
	s.quit()
	
def parseFile ( file ):    
    f = open( file )
    
    mailInfo = {}
    
    for line in f:
	line = line.rstrip( "\n" )
	fields = line.split("\t")
	
	if fields[4] not in mailInfo:
	    mailInfo[ fields[4] ] = []

        mailInfo[ fields[4] ].append( {
	    'Flowcell' : fields[0],
	    'LIMS Name' : fields[1],
	    'LIMS ID' : fields[2],
	    'Owner' : fields[3]
	} )
    f.close
    return mailInfo

def main():
    global options
    
    mails = {}
    
    parser = OptionParser()
    
    parser.add_option( "-f", "--file", help = "TAB-delimited file containing Flowcell, LIMS Name, LIMS ID, Owner & Owner Email" )
    parser.add_option( "-m", "--mail", help = "Email address of email sender" )
    
    ( options, otherArgs ) = parser.parse_args()
     
    mailInfo = parseFile( options.file )
    sendMails( mailInfo )
if __name__ == "__main__":
    main()