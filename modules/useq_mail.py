"""Module for sending HTML emails with optional attachments and logo."""

import mimetypes
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional

from modules.useq_template import TEMPLATE_PATH


def send_mail(subject: str, content: str, sender: str, receivers: List[str], attachments: Optional[Dict[str, str]] = None, logo: bool = True,):
    """
    Send an HTML email to one or more recipients with optional attachments.

    Args:
        subject (str): Email subject line.
        content (str): HTML content of the email body.
        sender (str): Email address of the sender.
        receivers (List[str]): List of recipient email addresses.
        attachments (Optional[Dict[str, str]]): Optional dictionary mapping attachment names to file paths.
        logo (bool): If True, attach the USEQ logo to the email.

    Raises:
        FileNotFoundError: If the logo file cannot be found when logo=True.
        SMTPException: If there's an error sending the email.
    """
    outer = MIMEMultipart()
    outer["Subject"] = str(subject)
    outer["From"] = sender
    outer["BCC"] = ", ".join(receivers)
    outer.attach(MIMEText(content, "html"))

    # Attach files if provided
    if attachments:
        for attachment_name, attachment_path in attachments.items():
            if not Path(attachment_path).is_file():
                continue

            _attach_file(outer, attachment_name, attachment_path)

    # Attach logo if requested
    if logo:
        _attach_logo(outer)

    # Send the email
    with smtplib.SMTP("localhost") as smtp_server:
        smtp_server.sendmail(sender, receivers, outer.as_string())


def _attach_file(outer: MIMEMultipart, attachment_name: str, attachment_path: str):
    """
    Attach a file to the email message.

    Args:
        outer (MIMEMultipart): The multipart email message to attach the file to.
        attachment_name (str): Name identifier for the attachment (used in Content-ID).
        attachment_path (str): File system path to the attachment.
    """
    file_name = Path(attachment_path).name
    ctype, encoding = mimetypes.guess_type(attachment_path)

    # Default to generic binary type if MIME type cannot be determined
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"

    maintype, subtype = ctype.split("/", 1)

    # Handle images differently from other file types
    if maintype == "image":
        with open(attachment_path, "rb") as fp:
            msg = MIMEImage(fp.read(), _subtype=subtype)
    else:
        with open(attachment_path, "rb") as fp:
            msg = MIMEBase(maintype, subtype)
            msg.set_payload(fp.read())
        # Encode the payload using Base64
        encoders.encode_base64(msg)

    msg.add_header("Content-Disposition", "attachment", filename=file_name)
    msg.add_header("Content-ID", f"<{attachment_name}>")
    outer.attach(msg)


def _attach_logo(outer: MIMEMultipart):
    """
    Attach the USEQ logo image to the email message.

    Args:
        outer (MIMEMultipart): The multipart email message to attach the logo to.

    Raises:
        FileNotFoundError: If the logo file cannot be found.
    """
    logo_path = Path(TEMPLATE_PATH) / "useq_logo.jpg"

    with open(logo_path, "rb") as fp:
        logo_image = MIMEImage(fp.read())

    logo_image.add_header("Content-ID", "<logo_image>")
    outer.attach(logo_image)
