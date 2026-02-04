from genologics.lims import Lims
from typing import List, Dict, Any, TextIO, Literal, Optional
def get_researchers(lims: Lims, out_file: TextIO):
    """
    Retrieve and write researcher information from LIMS.

    Args:
        lims (Lims): LIMS instance
        out_file (TextIO): A buffered text stream (either a file or stdout)
    """

    researchers = lims.get_researchers()
    out_file.write("LIMS ID;First Name;Last Name;Email;Username;Account Locked;Lab Name;Lab ID;Billing Street;Billing City;Billing State;Billing Country;Billing PostalCode;Billing Institution;Billing Department\n")
    for researcher in researchers:
        lab = researcher.lab
        line = []

        # Add basic researcher information
        line.append(str(researcher.id))
        line.append(str(researcher.first_name))
        line.append(str(researcher.last_name))
        line.append(str(researcher.email))

        # Add username with fallback
        try:
            line.append(str(researcher.username))
        except AttributeError:
            line.append('NA')

        # Add account lock status with fallback
        try:
            line.append(str(researcher.account_locked))
        except AttributeError:
            line.append('NA')

        # Add lab information
        line.append(str(lab.name))
        line.append(str(lab.id))

        # Format billing address
        billing_parts = [
            lab.billing_address.get('street'),
            lab.billing_address.get('city'),
            lab.billing_address.get('state'),
            lab.billing_address.get('country'),
            lab.billing_address.get('postalCode'),
            lab.billing_address.get('institution'),
            lab.billing_address.get('department')
        ]
        billing_address = ';'.join(str(part) if part is not None else 'None'
                                   for part in billing_parts)
        line.append(billing_address)

        # Print semicolon-delimited line
        out_file.write(';'.join(line)+"\n")


def run(lims, out_file):
    """Get all researcher metadata from LIMS."""
    get_researchers(lims, out_file)
