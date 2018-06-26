import argparse
import getpass
import xml.etree.cElementTree as ET
import datetime
import subprocess
#Full path to config slicer
CONFIG_SLICER='/opt/gls/clarity/tools/config-slicer/config-slicer-3.0.24.jar'
#URI for LIMS api
API_URI='https://usf-lims-test.op.umcutrecht.nl/api'
#Used for writing temporary XML file to
TMP_DIR='/tmp/'

def createConfigXML(barcodes):
    config = ET.Element("config",ApiVersion="v2,r20",ConfigSlicerVersion="3.0-compatible")
    reagent_types = ET.SubElement(config, "ReagentTypes")

    with open(barcodes, "r") as bc:
        for line in bc.readlines():
            cat,name,seq = line.rstrip().split(",")
            bc_name = "{0} ({1})".format(name,seq)
            bc_element = ET.SubElement(reagent_types, "rtp:reagent-type", name=bc_name)
            bc_element.set("xmlns:rtp","http://genologics.com/ri/reagenttype")
            type = ET.SubElement(bc_element, "special-type", name="Index")
            ET.SubElement(type, "attribute", value=seq, name="Sequence")
            ET.SubElement(bc_element,"reagent-category").text = cat

    tree = ET.ElementTree(config)

    xml_name = "{0}add_bc-{1}.xml".format(TMP_DIR,datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
    tree.write(xml_name, xml_declaration=True, encoding='utf-8', method="xml")

    return xml_name

def addBarcodes(barcode_xml, pw):

    print subprocess.check_output(
        "java -jar {0} -o importAndOverwrite -a {1} -u sboymans -p '{2} -k ".format(CONFIG_SLICER, API_URI, pw)
    )


def main():

    global args

    parser = argparse.ArgumentParser(prog='usf_add_barcodes', description='''
    This script is used to import a set of barcodes in Clarity LIMS. The barcode file has to be formatted like so:
    reagent-category,barcode-name,barcode-sequence (e.g. Illumina Nextera Index Kit v2,N701-S501,TAAGGCGA-GCGATCTA)
    The script is intended to be run by the glsjboss user but still requires an existing username + password.''')

    parser.add_argument('-b','--barcodes', help='File containing barcodes in the following format', required=True)
    parser.add_argument('-u','--user', help='Account name of user doing the import', required=True)
    args = parser.parse_args()

    pw = getpass.getpass("Please enter the password for account {0}:\n".format(args.user))

    barcode_xml = createConfigXML(args.barcodes)

    addBarcodes(barcode_xml, pw)


if __name__ == "__main__":

    main()
