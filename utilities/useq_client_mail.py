"""USEQ client mail functions."""

import sys
from typing import Dict, List, Optional, Set, TextIO

from genologics.lims import Lims
from genologics.entities import Researcher, Lab

from modules.useq_template import render_template
from modules.useq_mail import send_mail
from modules.useq_ui import query_yes_no

def parse_content(content: TextIO) -> Dict[str, str]:
    """
    Parse email content from CSV-like file.

    Expected format:
        subject,Your subject line here
        content,First line of content
        content,Second line of content

    Args:
        content: File object containing email subject and content lines.

    Returns:
        Dictionary with 'subject' and 'content' (HTML formatted) keys.
    """
    mail = {'subject': '', 'content': []}

    for line in content:
        line = line.rstrip()

        if not line:
            continue

        if line.startswith('subject'):
            mail['subject'] = line.split(",", 1)[1].strip()
        elif line.startswith('content'):
            mail['content'].append(line.split(",", 1)[1].strip())

    # Render content as HTML
    mail['content'] = render_template('client_mail_template.html',{'lines': mail['content']} )

    return mail


def confirm_send(mail: Dict[str, str], email_addresses: List[str]) -> bool:
    """
    Display email details and ask for user confirmation.

    Args:
        mail: Dictionary with 'subject' and 'content' keys.
        email_addresses: List of recipient email addresses.

    Returns:
        True if user confirms, False otherwise.
    """
    print("\nYou're about to send the following email:")
    print(f"Subject: {mail['subject']}")
    print(f"\nContent:\n{mail['content']}")
    print(f"\nTo ({len(email_addresses)} recipients):")
    print("\t" + "\n\t".join(sorted(email_addresses)))
    
    return query_yes_no("\nAre you sure? Please respond with (y)es or (n)o.")

def get_active_researcher_emails(researchers: List[Researcher],filter_labs: Optional[Set[str]] = None) -> List[str]:
    """
    Extract unique email addresses from active researchers.

    Args:
        researchers: List of Researcher objects.
        filter_labs: Optional set of lab URIs to filter by.
            If provided, only includes researchers from these labs.

    Returns:
        List of unique email addresses from active researchers.
    """
    emails = []
    seen_emails = set()

    for researcher in researchers:
        # Skip locked accounts
        if hasattr(researcher, 'account_locked') and researcher.account_locked:
            continue

        # Skip accounts without email
        if not hasattr(researcher, 'email') or not researcher.email:
            continue

        # Filter by lab if specified
        if filter_labs is not None:
            if not hasattr(researcher, 'lab') or researcher.lab.uri not in filter_labs:
                continue

        # Add unique emails only
        email = researcher.email.strip()
        if email and email not in seen_emails:
            emails.append(email)
            seen_emails.add(email)

    return emails


def send_to_all(lims: Lims,sender: str,content: TextIO,attachment: Optional[str] = None, **kwargs) -> None:
    """
    Send an email to all active researchers.

    Args:
        lims: LIMS instance.
        sender: Email address of the sender.
        content: File object containing email subject and content.
        attachment: Optional path to file attachment.
        **kwargs: Additional keyword arguments (unused, for compatibility).
    """
    # Parse email content
    mail = parse_content(content)

    # Get all active researchers' emails
    researchers = lims.get_researchers()
    receivers = get_active_researcher_emails(researchers)

    if not receivers:
        print("No active researchers found with valid email addresses.")
        return

    # Confirm and send
    if confirm_send(mail, receivers):
        send_mail(
            mail['subject'],
            mail['content'],
            sender,
            receivers,
            attachment
        )
        print(f"\nEmail sent successfully to {len(receivers)} recipients.")
    else:
        print("\nEmail send cancelled.")


def send_to_accounts(lims: Lims,sender: str,content: TextIO,name: str,attachment: Optional[str] = None,**kwargs) -> None:
    """
    Send an email to specific user accounts.

    Args:
        lims: LIMS instance.
        sender: Email address of the sender.
        content: File object containing email subject and content.
        name: Comma-separated list of usernames.
        attachment: Optional path to file attachment.
        **kwargs: Additional keyword arguments (unused, for compatibility).

    Raises:
        ValueError: If name parameter is not provided or empty.
    """
    if not name:
        raise ValueError("Parameter 'name' is required for sending to accounts.")

    # Parse email content
    mail = parse_content(content)

    # Get specified accounts' email addresses
    account_names = [n.strip() for n in name.split(',') if n.strip()]

    if not account_names:
        raise ValueError("No valid account names provided.")

    researchers = lims.get_researchers(username=account_names)
    receivers = get_active_researcher_emails(researchers)

    if not receivers:
        print(
            f"No active researchers found for accounts: "
            f"{', '.join(account_names)}"
        )
        return

    # Confirm and send
    if confirm_send(mail, receivers):
        send_mail(
            mail['subject'],
            mail['content'],
            sender,
            receivers,
            attachment
        )
        print(f"\nEmail sent successfully to {len(receivers)} recipients.")
    else:
        print("\nEmail send cancelled.")


def send_to_labs(lims: Lims,sender: str, content: TextIO,name: str, attachment: Optional[str] = None, **kwargs) -> None:
    """
    Send an email to all researchers in specified labs.

    Args:
        lims: LIMS instance.
        sender: Email address of the sender.
        content: File object containing email subject and content.
        name: Comma-separated list of lab names.
        attachment: Optional path to file attachment.
        **kwargs: Additional keyword arguments (unused, for compatibility).

    Raises:
        ValueError: If name parameter is not provided or empty.
    """
    if not name:
        raise ValueError("Parameter 'name' is required for sending to labs.")

    # Parse email content
    mail = parse_content(content)

    # Get specified labs
    lab_names = [n.strip() for n in name.split(',') if n.strip()]

    if not lab_names:
        raise ValueError("No valid lab names provided.")

    labs = lims.get_labs(name=lab_names)

    if not labs:
        print(f"No labs found with names: {', '.join(lab_names)}")
        return

    # Get lab URIs for filtering
    lab_uris = {lab.uri for lab in labs}

    # Get all researchers and filter by lab
    researchers = lims.get_researchers()
    receivers = get_active_researcher_emails(researchers, filter_labs=lab_uris)

    if not receivers:
        print(
            f"No active researchers found in labs: "
            f"{', '.join(lab_names)}"
        )
        return

    # Confirm and send
    if confirm_send(mail, receivers):
        send_mail(
            mail['subject'],
            mail['content'],
            sender,
            receivers,
            attachment
        )
        print(f"\nEmail sent successfully to {len(receivers)} recipients.")
    else:
        print("\nEmail send cancelled.")


def run(lims: Lims, sender: str, content: TextIO, mode: str, attachment: Optional[str] = None, name: Optional[str] = None) -> None:
    """
    Send emails based on specified mode.

    Args:
        lims: LIMS instance.
        sender: Email address of the sender.
        content: File object containing email subject and content.
        mode: Operation mode ('all', 'accounts', or 'labs').
        attachment: Optional path to file attachment.
        name: Required for 'accounts' and 'labs' modes.
            Comma-separated list of account usernames or lab names.

    Raises:
        ValueError: If mode is not recognized or required parameters missing.
    """
    operations = {
        'all': send_to_all,
        'accounts': send_to_accounts,
        'labs': send_to_labs,
    }

    if mode not in operations:
        raise ValueError(
            f"Unknown mode: '{mode}'. "
            f"Valid modes are: {', '.join(operations.keys())}"
        )

    operation = operations[mode]

    # Call the operation with appropriate parameters
    try:
        operation(
            lims=lims,
            sender=sender,
            content=content,
            name=name,
            attachment=attachment
        )
    except TypeError as e:
        # Handle missing required parameters
        if "required positional argument" in str(e) or "got an unexpected" in str(e):
            raise ValueError(
                f"Mode '{mode}' requires appropriate parameters. "
                f"Error: {e}"
            )
        raise
