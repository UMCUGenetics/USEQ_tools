"""Module for rendering Jinja2 templates."""

from pathlib import Path
from typing import Any, Dict

from jinja2 import Environment, FileSystemLoader

# Path configuration
_CURRENT_PATH = Path(__file__).parent
TEMPLATE_PATH = str(_CURRENT_PATH.parent / "resources")

# Jinja2 environment configuration
TEMPLATE_ENVIRONMENT = Environment(
    autoescape=False,
    loader=FileSystemLoader(TEMPLATE_PATH),
    trim_blocks=False,
)


def render_template(template_filename: str, data: Dict[str, Any]) -> str:
    """
    Render a Jinja2 template with the provided data.

    Args:
        template_filename (str): Name of the template file to render.
        data (Dict[str, Any]): Dictionary containing template variables and values.

    Returns:
        Rendered template as a string.

    Raises:
        jinja2.TemplateNotFound: If the template file doesn't exist.
        jinja2.TemplateError: If there's an error rendering the template.
    """
    return TEMPLATE_ENVIRONMENT.get_template(template_filename).render(data)
