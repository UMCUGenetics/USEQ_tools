"""USEQ client mail functions"""
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate
from modules.useq_mail import sendMail
import time

def parseContent(content):
    """Parse content csv file """
    mail = {'subject' : '', 'content' : []}

    for line in content.readlines():
        line = line.rstrip()
        if not line: continue

        if line.startswith('subject'):
            mail['subject'] = line.split(",",1)[1]
        elif line.startswith('content'):
            mail['content'].append( line.split(",",1)[1] )

    mail['content'] = renderTemplate('client_mail_template.html', {'lines':mail['content']})
    return mail

def check(mail,email_addresses ):
    print ("You're about to send the following email :")
    print ("Subject : " + str(mail['subject']))
    print ("Content : \n" + mail['content'])
    print ("To:")
    print ("\t".join( sorted(email_addresses) ))
    print ("Are you sure? Please respond with (y)es or (n)o.")

    yes = set(['yes','y', 'ye', ''])
    no = set(['no','n'])
    choice = input().lower()
    if choice in yes:
       choice = True
    elif choice in no:
       choice = False
    else:
       sys.stdout.write("Please respond with 'yes' or 'no'")

    return choice


def all(lims,sender,content,*args,**kwargs):
    """Sends an email to all active accounts (researchers)"""

    #Parse content file
    mail = parseContent(content)

    #Get all active client's emails
    receivers = []
    accounts = lims.get_researchers()
    for account in accounts:
        if hasattr(account, 'account_locked') and account.account_locked: continue
        if hasattr(account, 'email') and not account.email: continue
        if account.email not in receivers:
            receivers.append(account.email)

    if check(mail, receivers):
        sendMail(mail['subject'],mail['content'], sender ,receivers, kwargs['attachment'])

def accounts(lims,sender,content,*args,**kwargs):
    """Sends and email to the specified user accounts"""

    #Parse content file
    mail = parseContent(content)

    #Get all accounts' email addresses
    account_names = kwargs['name'].split(',')
    receivers = []
    accounts = lims.get_researchers(username=account_names)
    for account in accounts:
        if hasattr(account, 'account_locked') and account.account_locked: continue
        if hasattr(account, 'email') and not account.email: continue
        if account.email not in receivers:
            receivers.append(account.email)

    if check(mail, receivers):
        sendMail(mail['subject'],mail['content'], sender ,receivers, kwargs['attachment'])


def labs(lims,sender,content,**kwargs):
    """Sends an email to the specified groups"""

    #Parse content file
    mail = parseContent(content)

    #Get all groups' email addresses
    lab_names = kwargs['name'].split(',')
    labs =  lims.get_labs(name=lab_names)
    receivers = []
    lab_uris = []
    for lab in labs:
        lab_uris.append(lab.uri)

    accounts = lims.get_researchers()
    for account in accounts:
        if hasattr(account, 'account_locked') and account.account_locked: continue
        if hasattr(account, 'email') and not account.email: continue
        if account.email not in receivers and account.lab.uri in lab_uris:
            receivers.append(account.email)

    if check(mail, receivers):
        sendMail(mail['subject'],mail['content'], sender, receivers, kwargs['attachment'])

def run(lims, sender, content, mode, attachment=None, name=None):
    """Run all,accounts or labs function based on mode"""
    globals()[mode](lims,sender,content, name=name, attachment=attachment)
