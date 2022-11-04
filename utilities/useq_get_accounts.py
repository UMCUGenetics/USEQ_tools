
def getAccounts(lims,out_file):

    labs = lims.get_labs()
    out_file.write(
        "name;lims_id;billing_street;billing_city;billing_state;billing_country;billing_postalCode;billing_institution;billing_department;"
        "shipping_street;shipping_city;shipping_state;shipping_country;shipping_postalCode;shipping_institution;shipping_department;"
        "budget_nrs;vat_nr;deb_nr;supervisor_email;finance_email\n"
    )

    for lab in labs:
        budget_nrs = lab.udf.get('BudgetNrs','NA').replace('\n',',')
        out_file.write(
            f"{lab.name};{lab.id};{lab.billing_address.get('street','NA')};{lab.billing_address.get('city','NA')};{lab.billing_address.get('state','NA')};{lab.billing_address.get('country','NA')};"
            f"{lab.billing_address.get('postalCode','NA')};{lab.billing_address.get('institution','NA')};{lab.billing_address.get('department','NA')};"
            f"{lab.shipping_address.get('street','NA')};{lab.shipping_address.get('city','NA')};{lab.shipping_address.get('state','NA')};{lab.shipping_address.get('country','NA')};"
            f"{lab.shipping_address.get('postalCode','NA')};{lab.shipping_address.get('institution','NA')};{lab.shipping_address.get('department','NA')};"
            f"{budget_nrs};"
            f"{lab.udf.get('UMCU_VATNr','NA')};"
            f"{lab.udf.get('UMCU_DebNr','NA')};"
            f"{lab.udf.get('Supervisor Email','NA')};"
            f"{lab.udf.get('Finance Department Email','NA')}\n"
        )



def run(lims, out_file):
    """Gets all account meta data"""

    getAccounts(lims, out_file)
