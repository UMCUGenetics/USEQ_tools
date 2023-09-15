
def getResearchers(lims):

    researchers = lims.get_researchers()

    for researcher in researchers:
        lab = researcher.lab
        line = []
        line.append(u'{}'.format(researcher.id))
        line.append(u'{}'.format(researcher.first_name))
        line.append(u'{}'.format(researcher.last_name))
        line.append(u'{}'.format(researcher.email))

        try:
            line.append(u'{}'.format(researcher.username))
        except:
            line.append(u'{}'.format('NA'))

        try:
            line.append(u'{}'.format(researcher.account_locked))
        except:
            line.append(u'{}'.format('NA'))

        line.append(u'{}'.format(lab.name))
        line.append(u'{}'.format(lab.id))

        line.append(f"{lab.billing_address.get('street', None)};{lab.billing_address.get('city',None)};{lab.billing_address.get('state',None)};{lab.billing_address.get('country',None)};{lab.billing_address.get('postalCode',None)};{lab.billing_address.get('institution',None)};{lab.billing_address.get('department',None)}")


        print (';'.join( (v) for v in line))

        # line = ",".join(line)



def run(lims):
    """Gets all researcher meta data"""

    getResearchers(lims)
