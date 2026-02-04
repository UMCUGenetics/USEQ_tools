from genologics.lims import Lims
from typing import List, Dict, Any, TextIO, Literal, Optional

def get_accounts(lims: Lims, out_file: TextIO):
    """
    Retrieve and write lab account information to file.

    Args:
        lims (Lims): LIMS instance
        out_file (TextIO): A buffered text stream (either a file or stdout)
    """
    labs = lims.get_labs()

    # Write header row
    header = (
        "Name;LIMS ID;Billing Street;Billing City;Billing State;Billing Country;Billing Postal Code;Billing Institution;Billing Department;"
        "Shipping Street;Shipping City;Shipping State;Shipping Country;Shipping Postal Code;Shipping Institution;Shipping Department;"
        "Budget Numbers;VAT Number;Debtor Number;Supervisor Email;Finance Email\n"
    )
    out_file.write(header)

    # Write data for each lab
    for lab in labs:
        budget_nrs = lab.udf.get('BudgetNrs', 'NA')
        if budget_nrs:
            budget_nrs = budget_nrs.replace('\n', ',')
        else:
            budget_nrs = 'NA'

        # Extract billing address fields
        billing = _format_address(lab.billing_address)

        # Extract shipping address fields
        shipping = _format_address(lab.shipping_address)

        # Extract UDF fields
        vat_nr = lab.udf.get('UMCU_VATNr') or 'NA'
        deb_nr = lab.udf.get('UMCU_DebNr') or 'NA'
        supervisor_email = lab.udf.get('Supervisor Email') or 'NA'
        finance_email = lab.udf.get('Finance Department Email') or 'NA'

        # Write lab data row
        row = (
            f"{lab.name};{lab.id};{billing};{shipping};"
            f"{budget_nrs};{vat_nr};{deb_nr};{supervisor_email};{finance_email}\n"
        )
        out_file.write(row)


def _format_address(address):
    """Format address dictionary as semicolon-delimited string.

    Args:
        address: Dictionary containing address fields

    Returns:
        Semicolon-delimited string of address fields
    """
    fields = ['street', 'city', 'state', 'country', 'postalCode', 'institution', 'department']
    return ';'.join(address.get(field) or 'NA' for field in fields)


def run(lims, out_file):
    """Get all lab account metadata and write to file."""
    get_accounts(lims, out_file)
