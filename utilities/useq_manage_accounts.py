"""USEQ account management functions"""
import sys
import os
from csv import DictReader
from genologics.entities import Lab
import xml.etree.ElementTree as ET
from modules.useq_template import TEMPLATE_PATH,TEMPLATE_ENVIRONMENT,renderTemplate


def queryYesNo(question, default="no"):

    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")


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
    template_data['account_SupEmail'] = account.udf['Supervisor Email'] if 'Supervisor Email' in account.udf else ''
    template_data['account_FinEmail'] = account.udf['Finance Department Email'] if 'Finance Department Email' in account.udf else ''

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

    
    if queryYesNo("Is this correct  (y)es/(n)o ?"):
        account_XML = renderTemplate('account_template.xml', account_update)
        response = lims.put(account.uri, account_XML)
    else:
        sys.exit("Failed to update account {0}".format(acc))


def batch_edit(lims, csv):
    """Edit multiple existing accounts"""

    with open(csv, 'r') as c:
        csv_reader = DictReader(c, delimiter=";")

        for row in csv_reader:
            diff = {}
            lab_id = row['lims_id']
            lab = Lab(lims,id=lab_id)

            #Check all differences
            if row['name'] != lab.name : diff['account_name'] = (row['name'],lab.name)
            billing_street = 'None' if not lab.billing_address.get('street','NA') else lab.billing_address.get('street','NA')
            billing_city = 'None' if not lab.billing_address.get('city','NA') else lab.billing_address.get('city','NA')
            billing_state = 'None' if not lab.billing_address.get('state','NA') else lab.billing_address.get('state','NA')
            billing_country = 'None' if not lab.billing_address.get('country','NA') else lab.billing_address.get('country','NA')
            billing_postalCode = 'None' if not lab.billing_address.get('postalCode','NA') else lab.billing_address.get('postalCode','NA')
            billing_institution = 'None' if not lab.billing_address.get('institution','NA') else lab.billing_address.get('institution','NA')
            billing_department = 'None' if not lab.billing_address.get('department','NA') else lab.billing_address.get('department','NA')

            shipping_street = 'None' if not lab.shipping_address.get('street','NA') else lab.shipping_address.get('street','NA')
            shipping_city = 'None' if not lab.shipping_address.get('city','NA') else lab.shipping_address.get('city','NA')
            shipping_state = 'None' if not lab.shipping_address.get('state','NA') else lab.shipping_address.get('state','NA')
            shipping_country = 'None' if not lab.shipping_address.get('country','NA') else lab.shipping_address.get('country','NA')
            shipping_postalCode = 'None' if not lab.shipping_address.get('postalCode','NA') else lab.shipping_address.get('postalCode','NA')
            shipping_institution = 'None' if not lab.shipping_address.get('institution','NA') else lab.shipping_address.get('institution','NA')
            shipping_department = 'None' if not lab.shipping_address.get('department','NA') else lab.shipping_address.get('department','NA')

            if row['billing_street'] != billing_street : diff['billing_street'] = (billing_street,row['billing_street'])
            if row['billing_city'] != billing_city : diff['billing_city'] = (billing_city, row['billing_city'])
            if row['billing_state'] != billing_state : diff['billing_state'] = (billing_state,row['billing_state'])
            if row['billing_country'] != billing_country : diff['billing_country'] = (billing_country,row['billing_country'])
            if row['billing_postalCode'] != billing_postalCode : diff['billing_postalCode'] = (billing_postalCode,row['billing_postalCode'])
            if row['billing_institution'] != billing_institution : diff['billing_institution'] = (billing_institution,row['billing_institution'])
            if row['billing_department'] != billing_department : diff['billing_department'] = (billing_department,row['billing_department'])

            if row['shipping_street'] != shipping_street : diff['shipping_street'] = (shipping_street,row['shipping_street'])
            if row['shipping_city'] != shipping_city : diff['shipping_city'] = (shipping_city, row['shipping_city'])
            if row['shipping_state'] != shipping_state : diff['shipping_state'] = (shipping_state,row['shipping_state'])
            if row['shipping_country'] != shipping_country : diff['shipping_country'] = (shipping_country,row['shipping_country'])
            if row['shipping_postalCode'] != shipping_postalCode : diff['shipping_postalCode'] = (shipping_postalCode,row['shipping_postalCode'])
            if row['shipping_institution'] != shipping_institution : diff['shipping_institution'] = (shipping_institution,row['shipping_institution'])
            if row['shipping_department'] != shipping_department : diff['shipping_department'] = (shipping_department,row['shipping_department'])


            budget_nrs = lab.udf.get('BudgetNrs','NA').replace('\n',',')
            if row['budget_nrs'] != budget_nrs : diff['account_BudgetNrs'] = (budget_nrs, row['budget_nrs'])
            if row['vat_nr'] != lab.udf.get('UMCU_VATNr','NA') : diff['account_VATNr'] = (lab.udf.get('UMCU_VATNr','NA'), row['vat_nr'])
            if row['deb_nr'] != lab.udf.get('UMCU_DebNr','NA') : diff['account_DEBNr'] = (lab.udf.get('UMCU_DebNr','NA'), row['deb_nr'])
            if row['supervisor_email'] != lab.udf.get('Supervisor Email','NA') : diff['account_SupEmail'] = (lab.udf.get('Supervisor Email','NA'), row['supervisor_email'])
            if row['finance_email'] != lab.udf.get('Finance Department Email','NA') : diff['account_FinEmail'] = (lab.udf.get('Finance Department Email','NA'), row['finance_email'])

            if diff:
                print(f"The following changes were detected for account ({row['name']} : {row['lims_id']}):")
                for field in diff:
                    print(f"\t{field} : {diff[field][0]} -> {diff[field][1]}" )
                # print(diff)
                if queryYesNo("Do you want to make these changes (y)es/(n)o ?"):
                    account_update = {
                        'account_name' : diff['account_name'][1] if 'account_name' in diff else lab.name,
                        'account_website' : lab.website,
                        'account_BudgetNrs' : diff['account_BudgetNrs'][1] if 'account_BudgetNrs' in diff else budget_nrs,
                        'account_VATNr' : diff['account_VATNr'][1] if 'account_VATNr' in diff else lab.udf.get('UMCU_VATNr','NA'),
                        'account_DEBNr' : diff['account_DEBNr'][1] if 'account_DEBNr' in diff else lab.udf.get('UMCU_DebNr','NA'),
                        'account_SupEmail' : diff['account_SupEmail'][1] if 'account_SupEmail' in diff else lab.udf.get('Supervisor Email','NA'),
                        'account_FinEmail' : diff['account_FinEmail'][1] if 'account_FinEmail' in diff else lab.udf.get('Finance Department Email','NA'),
                        'billing_street' : diff['billing_street'][1] if 'billing_street' in diff else billing_street,
                        'billing_city' : diff['billing_city'][1] if 'billing_city' in diff else billing_city,
                        'billing_state' : diff['billing_state'][1] if 'billing_state' in diff else billing_state,
                        'billing_country' : diff['billing_country'][1] if 'billing_country' in diff else billing_country,
                        'billing_postalCode' : diff['billing_postalCode'][1] if 'billing_postalCode' in diff else billing_postalCode,
                        'billing_institution' : diff['billing_institution'][1] if 'billing_institution' in diff else billing_institution,
                        'billing_department' : diff['billing_department'][1] if 'billing_department' in diff else billing_department,

                        'shipping_street' : diff['shipping_street'][1] if 'shipping_street' in diff else shipping_street,
                        'shipping_city' : diff['shipping_city'][1] if 'shipping_city' in diff else shipping_city,
                        'shipping_state' : diff['shipping_state'][1] if 'shipping_state' in diff else shipping_state,
                        'shipping_country' : diff['shipping_country'][1] if 'shipping_country' in diff else shipping_country,
                        'shipping_postalCode' : diff['shipping_postalCode'][1] if 'shipping_postalCode' in diff else shipping_postalCode,
                        'shipping_institution' : diff['shipping_institution'][1] if 'shipping_institution' in diff else shipping_institution,
                        'shipping_department' : diff['shipping_department'][1] if 'shipping_department' in diff else shipping_department
                    }
                    account_XML = renderTemplate('account_template.xml', account_update)
                    lims.put(lab.uri, account_XML)


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
