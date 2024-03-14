import re
from functools import lru_cache
from pathlib import Path

from bs4 import BeautifulSoup

DEFAULT_DATA_ROOT = Path("./data")


@lru_cache
def get_name_from_soup(soup: BeautifulSoup):
    elements = soup.find_all("input", {"class": "input--full"})
    if len(elements) != 1:
        raise NotImplementedError(
            f"Unexpected number of input elements (expected 1): {elements!r}"
        )
    return elements[0].get("value")


AREA_PATTERN = (
    r"(?:Properties (?:To Rent|For Sale) (?:in|near) )?{}, (.*?)(:?, within .*)?$"
)


def get_area_from_text(display_name_full: str, display_name_short: str):
    # area = re.match(f"{display_name_short}, (.*)$", display_name_full)
    area = re.match(
        AREA_PATTERN.format(re.escape(display_name_short)), display_name_full
    )
    if area:
        return area.group(1)
    return None


def get_area_from_soup(soup: BeautifulSoup):
    name = get_name_from_soup(soup)
    elements = soup.find_all("h1", {"class": "searchTitle-heading"})
    if len(elements) != 1:
        raise NotImplementedError(
            f"Unexpected number of headings (expected 1): {elements!r}"
        )
    text_full = elements[0].text
    return get_area_from_text(text_full, name)
