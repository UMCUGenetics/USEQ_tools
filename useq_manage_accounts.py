import glsapiutil
from jinja2 import Environment, FileSystemLoader
import argparse
from xml.dom.minidom import parseString, parse
import os
import urllib
import urllib2
import getpass

HOSTNAME = ''
VERSION = ''
BASE_URI = ''
api = None
args = None
PATH = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_ENVIRONMENT = Environment(
    autoescape=False,
    loader=FileSystemLoader(os.path.join(PATH, 'resources')),
    trim_blocks=False)
CACHE={}
def renderTemplate(template_filename, data):
    """Render Jinja template."""
    return TEMPLATE_ENVIRONMENT.get_template(template_filename).render(data)

def setupGlobalsFromURI( uri ):

	global HOSTNAME
	global VERSION
	global BASE_URI

	tokens = uri.split( "/" )
	HOSTNAME = "/".join(tokens[0:3])
	VERSION = tokens[4]
	BASE_URI = "/".join(tokens[0:5]) + "/"

def getObjectDOM( uri ):

	global CACHE
	#global api
	if uri not in CACHE.keys():

		thisXML = api.getResourceByURI( uri )

		thisDOM = parseString( thisXML )
		CACHE[ uri ] = thisDOM

	return CACHE[ uri ]


def DOMToCsv(account_DOM):
    #Read in account template csv file
    template_data = {}
    with open(os.path.join(PATH, 'resources','account_template.csv'), 'r') as template:
        header = ''
        for line in template.readlines():
            fields =line.rstrip().split(",")
            if not fields[0]:continue #skip empty lines
            if '[' in fields[0]: #new section
                header = fields[0].replace('[','').replace(']','')
                continue
            xml_name = fields[0].split("_")[1]
            if 'general' in header:
                if account_DOM.getElementsByTagName(xml_name)[0].firstChild:
                    template_data[fields[0]] = account_DOM.getElementsByTagName(xml_name)[0].firstChild.data
                else:
                    template_data[fields[0]] = ''
            elif 'udfs' in header:
                if api.getUDF(account_DOM, xml_name):
                    template_data[fields[0]] = api.getUDF(account_DOM, xml_name).replace("\n",",")
                else:
                    template_data[fields[0]] = ''
            else:
                if account_DOM.getElementsByTagName( header )[0].getElementsByTagName( xml_name)[0].firstChild:
                    template_data[fields[0]] = account_DOM.getElementsByTagName( header )[0].getElementsByTagName( xml_name )[0].firstChild.data
                else:
                    template_data[fields[0]] = ''
    csv_content = renderTemplate('account_template.csv', template_data)
    return csv_content

def csvToDOM(csv):
    template_data = {}
    header = ''
    for line in csv.readlines():
        fields =line.rstrip().split(",")
        if not fields[0]:continue #skip empty lines
        if '[' in fields[0]: #new section
            header = fields[0].replace('[','').replace(']','')
            continue#Returns file name of file containing account info

        if fields[0] == 'account_BudgetNrs':
            template_data[fields[0]] = "\n".join(fields[1:])
        else:
            template_data[fields[0]] = " ".join(fields[1:])

    # print template_data
    xml_contents = renderTemplate('account_template.xml', template_data)
    # return xml_contents
    account_DOM = parseString(xml_contents)
    return account_DOM

#Returns account info in csv format
def getAccount(account_id):
    account_csv = ''
    try:
        account_id = int(account_id)
        account_DOM = getObjectDOM("{0}labs/{1}".format(BASE_URI,account_id))
        account_csv = DOMToCsv(account_DOM)
    except ValueError:
        account_DOM = getObjectDOM("{0}labs/?name={1}".format(BASE_URI,urllib.quote(account_id)))
        labs = account_DOM.getElementsByTagName("lab")
        if len(labs) > 1:
            print "Warning : More than one hits found for '{0}', using the first one.".format(account_id)
            account_uri = account_DOM.getElementsByTagName("lab")[0].getAttribute("uri")
        else:
            account_uri = account_DOM.getElementsByTagName("lab")[0].getAttribute("uri")

        account_DOM = getObjectDOM(account_uri)
        account_csv = DOMToCsv(account_DOM)

    return  account_csv


def getAccountNames():

    account_names = []
    account_DOM = getObjectDOM(BASE_URI + 'labs/')
    for name_element in account_DOM.getElementsByTagName('name'):
        account_names.append(name_element.firstChild.data)

    return account_names

def createAccount(csv):

    account_DOM = csvToDOM(csv)
    if account_DOM.getElementsByTagName('name')[0].firstChild.data in getAccountNames():
        print "Account name already exists, aborting!"
    else:
        response = api.createObject(account_DOM.toxml(), BASE_URI + 'labs/')
        response_DOM = parseString(response)
        account_id = response_DOM.getElementsByTagName('lab:lab')[0].getAttribute('uri').split("/")[-1]
        print "Succesfully create account {0} with id {1}".format(account_DOM.getElementsByTagName('name')[0].firstChild.data, account_id)

def modifyAccount(csv):
    account_id = csv.name.split("/")[-1].split(".")[0]
    account_DOM = csvToDOM(csv)

    print "You are about to change :"
    print getAccount(account_id)
    print "\n#####IN TO ####\n"

    print DOMToCsv(account_DOM)

    print "Is this correct (Y/N)?"
    go = raw_input()
    if go == 'Y':
        response = api.updateObject(account_DOM.toxml(), BASE_URI + 'labs/' + account_id)
        response_DOM = parseString(response)
        account_id = response_DOM.getElementsByTagName('lab:lab')[0].getAttribute('uri').split("/")[-1]
        print "Succesfully update account {0} with id {1}".format(account_DOM.getElementsByTagName('name')[0].firstChild.data, account_id)
    else:
        print "Failed to updated account with id {0}".format(account_id)

def main():

    global args
    global api

    #Set up command line arguments
    parser = argparse.ArgumentParser(prog='useq_manage_accounts', description='''
    Workaround script to manage accounts in LIMS 5.*''')
    parser.add_argument('-u','--user', help='User name', required=True)
    parser.add_argument('-w','--work_dir', help='Used for reading / writing files from/to', required=True)
    parser.add_argument('-g','--get', help='Get account by id or name)')
    parser.add_argument('-c','--create', help='Create an account. Requires a csv file as input.', nargs='?', type=argparse.FileType('r'))
    parser.add_argument('-m','--modify', help='Modify and account. Requires a csv file as input. User the account LIMSID as file name (e.g. 45.csv)', nargs='?', type=argparse.FileType('r'))
    parser.add_argument('-a','--api', help='LIMS API URI (e.g. https://usf-lims-test.op.umcutrecht.nl/api/v2/ )', required=True)
    args = parser.parse_args()

    pw = getpass.getpass("Please enter the password for account {0}:\n".format(args.user))
    #Set up handy functions
    setupGlobalsFromURI( args.api )
    api = glsapiutil.glsapiutil()
    api.setHostname( HOSTNAME )
    api.setVersion( VERSION )
    api.setup( args.user, pw )


    if args.get:
        print "{0}".format(getAccount(args.get))
    elif args.create:
        createAccount(args.create)
    elif args.modify:
        modifyAccount(args.modify)
    else:
        print "No valid mode provided (get, create, modify)"



if __name__ == "__main__":
    main()
