"""USEQ account management functions"""
import sys
import os
from genologics.entities import Lab
import xml.etree.ElementTree as ET
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate

def parseAccountCSV(csv):
    account = {}
    with open(csv, 'r') as csv_file:
        for line in csv_file.readlines():
            fields = line.rstrip().split(",")
            if not fields[0]:continue #skip empty lines
            if '[' in fields[0]: #new section
                continue
            if fields[0] == 'account_BudgetNrs':
                account[fields[0]] = "\n".join(fields[1:])
            else:
                account[fields[0]] = " ".join(fields[1:])
    return account


def getAccountCSV(account):
    template_data = {}
    template_data['account_name'] = account.name
    template_data['account_website'] = account.website

    template_data['account_BudgetNrs'] = account.udf['BudgetNrs'].replace("\n",",")
    template_data['account_VATNr'] = account.udf['UMCU_VATNr'] if 'UMCU_VATNr' in account.udf else ''
    template_data['account_DEBNr'] = account.udf['UMCU_DebNr'] if 'UMCU_DebNr' in account.udf else ''

    template_data['billing_street'] = account.billing_address['street']
    template_data['billing_city'] = account.billing_address['city']
    template_data['billing_state'] = account.billing_address['state']
    template_data['billing_country'] = account.billing_address['country']
    template_data['billing_postalCode'] = account.billing_address['postalCode']
    template_data['billing_institution'] = account.billing_address['institution']
    template_data['billing_department'] = account.billing_address['department']

    template_data['shipping_street'] = account.shipping_address['street']
    template_data['shipping_city'] = account.shipping_address['city']
    template_data['shipping_state'] = account.shipping_address['state']
    template_data['shipping_country'] = account.shipping_address['country']
    template_data['shipping_postalCode'] = account.shipping_address['postalCode']
    template_data['shipping_institution'] = account.shipping_address['institution']
    template_data['shipping_department'] = account.shipping_address['department']
    return renderTemplate('account_template.csv', template_data) + "\n"


def getAccount(lims, acc):
    account = None
    try:
        acc_id = int(acc)
        account = Lab(lims,id=acc)

    except ValueError:
        accounts = lims.get_labs(name=acc)
        if len(accounts) > 1:
            sys.exit("Found multiple accounts matching {0}, please use the ID instead.".format(acc))
        account = accounts[0]
    return account

def create(lims, csv):
    """Create a new account, does not support billing and shipping address yet!!!"""
    account_info = parseAccountCSV(csv)

    if not lims.get_labs(name=account_info['account_name']):
        #Lab.create does not support creating billing/shipping addreses, so using post function instead
        account_XML = renderTemplate('account_template.xml', account_info)
        response = lims.post(lims.get_uri(Lab._URI), account_XML)

        account_name = response.findall('name')[0].text
        account_id = response.attrib['uri'].split("/")[-1]

        print ("Account {0} with ID {1} succesfully created".format(account_name, account_id))
    else:
        sys.exit("Account with name '{0}' already exists".format(account_info['account_name']))

def edit(lims, csv, acc):
    """Edit existing account"""
    account = getAccount(lims,acc)

    print ("You are about to change :")
    print (getAccountCSV(account))
    print ("\n#####IN TO ####\n")

    csv_content = ''
    with open(csv, 'r') as csv_file:
        csv_content = csv_file.read()

    print (csv_content)
    account_update = parseAccountCSV(csv)

    print ("Is this correct (Y/N)?")
    go = input()
    if go == 'Y':
        account_XML = renderTemplate('account_template.xml', account_update)
        response = lims.put(account.uri, account_XML)
    else:
        sys.exit("Failed to update account {0}".format(acc))

def retrieve(lims, csv, acc):
    """Saves matching account to csv"""
    account = getAccount(lims,acc)

    print ("Writing results to {0}".format(csv))
    # unicode(, "utf8")
    with open(csv, 'w') as csv_file:
        csv_file.write(  getAccountCSV(account) )

def run(lims, mode, csv, lab=None):
    """Run create,edit or retrieve function based on mode"""
    if lab:
        globals()[mode](lims, csv, lab)
    else:
        globals()[mode](lims, csv)
