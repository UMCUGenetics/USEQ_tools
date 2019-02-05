from useq_template import TEMPLATE_PATH
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders
import mimetypes
import smtplib

def sendMail(subject, content, sender ,receivers, attachments=None):
    """Send email to one or more email addresses, attachment is optional"""
    outer = MIMEMultipart()
    outer[ "Subject" ] = str(subject)
    outer[ "From" ] = sender
    outer[ "BCC" ] = ", ".join(receivers)
    outer.attach( MIMEText(content.encode('utf-8'), 'html') )

    if attachments:
        for attachment_name, attachment in attachments.iteritems():
            file_name = attachment.split('/')[-1]
            ctype, encoding = mimetypes.guess_type(attachment)
            if ctype is None or encoding is not None:
                # No guess could be made, or the file is encoded (compressed), so
                # use a generic bag-of-bits type.
                ctype = 'application/octet-stream'
            maintype, subtype = ctype.split('/', 1)
            if maintype == 'image':
                fp = open(attachment, 'rb')
                msg = MIMEImage(fp.read(), _subtype=subtype)
                fp.close()
            else:
                fp = open(attachment, 'rb')
                msg = MIMEBase(maintype, subtype)
                msg.set_payload(fp.read())
                fp.close()
                # Encode the payload using Base64
                encoders.encode_base64(msg)

            msg.add_header('Content-Disposition', 'attachment', filename=file_name)
            msg.add_header('Content-ID', '<{}>'.format(attachment_name))
            outer.attach(msg)

    #read the logo and add it to the email
    fp = open(TEMPLATE_PATH+'/useq_logo.jpg', 'rb')
    logo_image = MIMEImage(fp.read())
    fp.close()
    logo_image.add_header('Content-ID', '<logo_image>')
    outer.attach(logo_image)

    s = smtplib.SMTP( "localhost" )
    s.sendmail( sender, receivers, outer.as_string() )
    s.quit()
