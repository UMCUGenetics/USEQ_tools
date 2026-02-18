"""USEQ account management functions."""

import sys
from csv import DictReader
from pathlib import Path
from typing import Dict, Optional, Tuple

from genologics.entities import Lab
from genologics.lims import Lims

from modules.useq_template import render_template
from modules.useq_ui import query_yes_no

def parse_account_csv(csv_path: str) -> Dict[str, str]:
    """
    Parse account information from a CSV file.

    Args:
        csv_path (str): Path to the CSV file.

    Returns:
        Dictionary containing account information.
    """
    account = {}

    with open(csv_path, 'r', encoding='utf-8') as csv_file:
        for line in csv_file:
            fields = [f.strip() for f in line.rstrip().split(",")]

            # Skip empty lines
            if not fields[0]:
                continue

            # Skip section headers
            if '[' in fields[0]:
                continue

            # Handle budget numbers with newlines
            if fields[0] == 'account_BudgetNrs':
                account[fields[0]] = "\n".join(fields[1:])
            else:
                account[fields[0]] = " ".join(fields[1:])

    return account


def _get_address_value(address: Dict[str, str],key: str,default: str = '') -> str:
    """
    Safely get address value or return default.

    Args:
        address (Dict[str, str]): Address dictionary.
        key (str): Key to retrieve.
        default (str): Default value if key not found.

    Returns:
        Address value or default.
    """
    value = address.get(key, default)
    return value if value else default


def get_account_csv(account: Lab) -> str:
    """
    Convert account object to CSV format string.

    Args:
        account (Lab): Lab account object from LIMS.

    Returns:
        CSV formatted string of account information.
    """
    template_data = {
        'account_name': account.name,
        'account_website': account.website,
        'account_BudgetNrs': account.udf.get('BudgetNrs', '').replace("\n", ","),
        'account_VATNr': account.udf.get('UMCU_VATNr', ''),
        'account_DEBNr': account.udf.get('UMCU_DebNr', ''),
        'account_SupEmail': account.udf.get('Supervisor Email', ''),
        'account_FinEmail': account.udf.get('Finance Department Email', ''),
    }

    # Add billing address
    for field in ['street', 'city', 'state', 'country', 'postalCode',
                  'institution', 'department']:
        key = f'billing_{field}'
        template_data[key] = _get_address_value(
            account.billing_address, field
        )

    # Add shipping address
    for field in ['street', 'city', 'state', 'country', 'postalCode',
                  'institution', 'department']:
        key = f'shipping_{field}'
        template_data[key] = _get_address_value(
            account.shipping_address, field
        )

    return render_template('account_template.csv', template_data) + "\n"


def get_account(lims: Lims, acc: str) -> Lab:
    """
    Retrieve account by ID or name.

    Args:
        lims (Lims): LIMS instance.
        acc (str): Account ID or name.

    Returns:
        Lab account object.

    Raises:
        SystemExit: If multiple accounts found with same name or none found.
    """
    try:
        acc_id = acc
        return Lab(lims, id=acc_id)
    except ValueError:
        accounts = lims.get_labs(name=acc)

        if len(accounts) > 1:
            sys.exit(
                f"Found multiple accounts matching '{acc}', "
                "please use the ID instead."
            )
        elif len(accounts) == 0:
            sys.exit(f"No account found with name '{acc}'.")

        return accounts[0]


def create(lims: Lims, csv_path: str) -> None:
    """
    Create a new account from CSV file.

    Args:
        lims (Lims): LIMS instance.
        csv_path (str): Path to CSV file with account information.

    Raises:
        SystemExit: If account with same name already exists.
    """
    account_info = parse_account_csv(csv_path)
    account_name = account_info['account_name']

    if lims.get_labs(name=account_name):
        sys.exit(f"Account with name '{account_name}' already exists.")

    # Lab.create doesn't support billing/shipping addresses, use POST
    account_xml = render_template('account_template.xml', account_info)
    response = lims.post(lims.get_uri(Lab._URI), account_xml)

    created_name = response.findall('name')[0].text
    account_id = response.attrib['uri'].split("/")[-1]

    print(
        f"Account '{created_name}' with ID {account_id} "
        "successfully created."
    )


def edit(lims: Lims, csv_path: str, acc: str) -> None:
    """
    Edit existing account from CSV file.

    Args:
        lims (Lims): LIMS instance.
        csv_path (str): Path to CSV file with updated account information.
        acc (str): Account ID or name to edit.

    Raises:
        SystemExit: If user cancels the operation.
    """
    account = get_account(lims, acc)

    print("You are about to change:")
    print(get_account_csv(account))
    print("\n##### INTO #####\n")

    with open(csv_path, 'r', encoding='utf-8') as csv_file:
        csv_content = csv_file.read()
    print(csv_content)

    if not query_yes_no("Is this correct (y)es/(n)o?"):
        sys.exit(f"Failed to update account {acc}.")

    account_update = parse_account_csv(csv_path)
    account_xml = render_template('account_template.xml', account_update)
    lims.put(account.uri, account_xml)
    print(f"Account {acc} successfully updated.")


def _normalize_address_value(value: Optional[str]) -> str:
    """
    Normalize address value for comparison.

    Args:
        value (Optional[str]): Address field value.

    Returns:
        Normalized value ('None' if empty/None, otherwise the value).
    """
    if not value or value == 'NA':
        return 'None'
    return value


def _get_address_fields(lab: Lab,address_type: str) -> Dict[str, str]:
    """
    Extract all address fields from a lab account.

    Args:
        lab (Lab): Lab account object.
        address_type (str): Either 'billing' or 'shipping'.

    Returns:
        Dictionary of normalized address fields.
    """
    address = (
        lab.billing_address if address_type == 'billing'
        else lab.shipping_address
    )

    fields = {}
    for field in ['street', 'city', 'state', 'country', 'postalCode',
                  'institution', 'department']:
        key = f'{address_type}_{field}'
        fields[key] = _normalize_address_value(address.get(field, 'NA'))

    return fields


def _detect_changes(row: Dict[str, str],lab: Lab) -> Dict[str, Tuple[str, str]]:
    """
    Detect differences between CSV row and existing lab account.

    Args:
        row (Dict[str, str]): CSV row with new values.
        lab (Lab): Existing lab account.

    Returns:
        Dictionary of changes: {field_name: (old_value, new_value)}.
    """
    diff = {}

    # Check name
    if row['name'] != lab.name:
        diff['account_name'] = (lab.name, row['name'])

    # Check addresses
    billing_fields = _get_address_fields(lab, 'billing')
    shipping_fields = _get_address_fields(lab, 'shipping')

    for field, value in billing_fields.items():
        csv_key = field  # billing_street, etc.
        if row.get(csv_key, 'None') != value:
            diff[field] = (value, row.get(csv_key, 'None'))

    for field, value in shipping_fields.items():
        csv_key = field  # shipping_street, etc.
        if row.get(csv_key, 'None') != value:
            diff[field] = (value, row.get(csv_key, 'None'))

    # Check UDFs
    budget_nrs = lab.udf.get('BudgetNrs', 'NA').replace('\n', ',')
    if row.get('budget_nrs', '') != budget_nrs or ',' in row.get(
        'budget_nrs', ''
    ):
        diff['account_BudgetNrs'] = (budget_nrs, row.get('budget_nrs', ''))

    udf_mappings = {
        'vat_nr': ('UMCU_VATNr', 'account_VATNr'),
        'deb_nr': ('UMCU_DebNr', 'account_DEBNr'),
        'supervisor_email': ('Supervisor Email', 'account_SupEmail'),
        'finance_email': ('Finance Department Email', 'account_FinEmail'),
    }

    for csv_key, (udf_key, diff_key) in udf_mappings.items():
        current_value = lab.udf.get(udf_key, 'NA')
        if row.get(csv_key, '') != current_value:
            diff[diff_key] = (current_value, row.get(csv_key, ''))

    return diff


def _build_account_update(diff: Dict[str, Tuple[str, str]],lab: Lab) -> Dict[str, str]:
    """
    Build account update dictionary from detected changes.

    Args:
        diff (Dict[str, Tuple[str, str]]): Dictionary of detected changes.
        lab (Lab): Existing lab account.

    Returns:
        Dictionary ready for XML template rendering.
    """
    billing_fields = _get_address_fields(lab, 'billing')
    shipping_fields = _get_address_fields(lab, 'shipping')
    budget_nrs = lab.udf.get('BudgetNrs', 'NA').replace('\n', ',')

    account_update = {
        'account_name': diff.get('account_name', (None, lab.name))[1],
        'account_website': lab.website,
        'account_BudgetNrs': diff.get('account_BudgetNrs', (None, budget_nrs) )[1].replace(',', '\n'),
        'account_VATNr': diff.get('account_VATNr', (None, lab.udf.get('UMCU_VATNr', 'NA') ) )[1],
        'account_DEBNr': diff.get('account_DEBNr', (None, lab.udf.get('UMCU_DebNr', 'NA') ) )[1],
        'account_SupEmail': diff.get('account_SupEmail', (None, lab.udf.get('Supervisor Email', 'NA') ) )[1],
        'account_FinEmail': diff.get('account_FinEmail', (None, lab.udf.get('Finance Department Email', 'NA') ) )[1],
    }

    # Add address fields
    for field in billing_fields:
        account_update[field] = diff.get(field, (None, billing_fields[field] ) )[1]

    for field in shipping_fields:
        account_update[field] = diff.get(field, (None, shipping_fields[field]) )[1]

    return account_update


def batch_edit(lims: Lims, csv_path: str) -> None:
    """
    Edit multiple existing accounts from CSV file.

    Args:
        lims (Lims): LIMS instance.
        csv_path (str): Path to CSV file with account updates.
            Expected columns: lims_id, name, billing/shipping fields,
            budget_nrs, vat_nr, deb_nr, supervisor_email, finance_email.
    """
    with open(csv_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = DictReader(csv_file, delimiter=";")

        for row in csv_reader:
            lab_id = row['lims_id']
            lab = Lab(lims, id=lab_id)

            # Detect all changes
            diff = _detect_changes(row, lab)

            if not diff:
                continue

            # Display changes
            print(
                f"\nThe following changes were detected for account "
                f"({row['name']}: {lab_id}):"
            )
            for field, (old_val, new_val) in diff.items():
                print(f"  {field}: {old_val} -> {new_val}")

            # Confirm and apply changes
            if query_yes_no("Do you want to make these changes (y)es/(n)o?"):
                account_update = _build_account_update(diff, lab)
                account_xml = render_template(
                    'account_template.xml', account_update
                )
                lims.put(lab.uri, account_xml)
                print(f"Account {lab_id} successfully updated.")
            else:
                print(f"Skipped account {lab_id}.")


def retrieve(lims: Lims, csv_path: str, acc: str) -> None:
    """
    Save account information to CSV file.

    Args:
        lims (Lims): LIMS instance.
        csv_path (str): Path where CSV file will be saved.
        acc (str): Account ID or name to retrieve.
    """
    account = get_account(lims, acc)

    print(f"Writing results to {csv_path}")

    with open(csv_path, 'w', encoding='utf-8') as csv_file:
        csv_file.write(get_account_csv(account))

    print(f"Account information saved successfully.")


def run(lims: Lims, mode: str, csv_path: str, lab: Optional[str] = None) -> None:
    """
    Run create, edit, retrieve, or batch_edit function based on mode.

    Args:
        lims (Lims): LIMS instance.
        mode (str): Operation mode ('create', 'edit', 'retrieve', 'batch_edit').
        csv_path (str): Path to CSV file.
        lab (Optional[str]): Optional account ID or name (required for edit and retrieve).

    Raises:
        ValueError: If mode is not recognized.
    """
    operations = {
        'create': lambda: create(lims, csv_path),
        'edit': lambda: edit(lims, csv_path, lab) if lab else None,
        'retrieve': lambda: retrieve(lims, csv_path, lab) if lab else None,
        'batch_edit': lambda: batch_edit(lims, csv_path),
    }

    if mode not in operations:
        raise ValueError(
            f"Unknown mode: '{mode}'. "
            f"Valid modes are: {', '.join(operations.keys())}"
        )

    operation = operations[mode]

    if operation() is None and lab is None:
        raise ValueError(f"Mode '{mode}' requires a lab parameter.")
