import sys

# Constants
DEFAULT_RESPONSE = "no"
VALID_RESPONSES = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
PROMPT_MAP = {
    None: " [y/n] ",
    "yes": " [Y/n] ",
    "no": " [y/N] "
}

def query_yes_no(question: str, default: str = "no") -> bool:
    """
    Ask a yes/no question and return the user's response.

    Args:
        question (str): The question to ask the user.
        default (str): The default answer if user just presses Enter.

    Returns:
        True if user responds yes, False if no.

    Raises:
        ValueError: If default is not 'yes', 'no', or None.
    """
    if default not in PROMPT_MAP:
        raise ValueError(f"Invalid default answer: '{default}'")

    prompt = PROMPT_MAP[default]

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()

        if default is not None and choice == "":
            return VALID_RESPONSES[default]
        elif choice in VALID_RESPONSES:
            return VALID_RESPONSES[choice]
        else:
            sys.stdout.write(
                "Please respond with 'yes' or 'no' (or 'y' or 'n').\n"
            )
