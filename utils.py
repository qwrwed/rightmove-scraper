import re
from functools import lru_cache

from bs4 import BeautifulSoup

from constants import AREA_PATTERN


@lru_cache
def get_name_from_soup(soup: BeautifulSoup):
    elements = soup.find_all("input", {"class": "input--full"})
    if len(elements) != 1:
        raise NotImplementedError(
            f"Unexpected number of input elements (expected 1): {elements!r}"
        )
    return elements[0].get("value")


def get_area_from_soup(soup: BeautifulSoup):
    name = get_name_from_soup(soup)
    elements = soup.find_all("h1", {"class": "searchTitle-heading"})
    if len(elements) != 1:
        raise NotImplementedError(
            f"Unexpected number of headings (expected 1): {elements!r}"
        )
    text_full = elements[0].text
    area = re.match(AREA_PATTERN.format(name), text_full)
    if area:
        return area.group(1)
    return None
