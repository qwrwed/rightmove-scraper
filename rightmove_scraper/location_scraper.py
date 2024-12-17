import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from itertools import count
from math import isinf
from pathlib import Path
from pprint import pprint
from typing import Any, Iterable, TypedDict
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from requests import HTTPError
from tqdm import tqdm
from utils_python import (
    dump_data,
    make_get_request_to_url,
    print_tqdm,
    read_list_from_file,
)

from rightmove_scraper.utils import snake_to_camel_case

LOGGER = logging.getLogger(__name__)


class LocationType(StrEnum):
    STATION = "STATION"
    REGION = "REGION"
    OUTCODE = "OUTCODE"
    POSTCODE = "POSTCODE"


class Channel(StrEnum):
    RENT = "property-to-rent"
    BUY = "property-for-sale"


DEFAULT_CHANNEL = Channel.RENT


def make_url_query(**query: Any) -> str:
    query_dict = {snake_to_camel_case(k): v for k, v in query.items()}
    return urlencode(query_dict)


def make_scrape_url(
    location_identifier: str,
    channel: Channel = DEFAULT_CHANNEL,
    **query: Any,
) -> str:
    url_base = f"https://www.rightmove.co.uk/{channel}/find.html"
    url_query = make_url_query(
        location_identifier=location_identifier,
        **query,
    )
    return f"{url_base}?{url_query}"


def make_api_url(
    location_identifier: str,
    channel: Channel = DEFAULT_CHANNEL,
    **query: Any,
) -> str:
    url_base = "https://www.rightmove.co.uk/api/_search"
    url_query = make_url_query(
        location_identifier=location_identifier,
        channel=channel,
        **query,
    )
    return f"{url_base}?{url_query}"


AREA_PATTERN = r"(?:Properties (?:To Rent|For Sale) (?:in|near) )?{}, (.*?)(:?, within .*)?$"


def get_area_from_text(
    display_name_full: str,
    display_name_short: str,
) -> str:
    # area = re.match(f"{display_name_short}, (.*)$", display_name_full)
    area = re.match(AREA_PATTERN.format(re.escape(display_name_short)), display_name_full)
    if area:
        return area.group(1)
    raise NotImplementedError


@lru_cache
def get_name_from_soup(soup: BeautifulSoup) -> str:
    elements = soup.find_all("input", {"class": "input--full"})
    if len(elements) != 1:
        raise NotImplementedError(f"Unexpected number of input elements (expected 1): {elements!r}")

    name = elements[0].get("value")
    if isinstance(name, str):
        return name
    raise NotImplementedError


def get_area_from_soup(soup: BeautifulSoup) -> str:
    name = get_name_from_soup(soup)
    elements = soup.find_all("h1", {"class": "searchTitle-heading"})
    if len(elements) != 1:
        raise NotImplementedError(f"Unexpected number of headings (expected 1): {elements!r}")
    text_full = elements[0].text
    return get_area_from_text(text_full, name)


def get_indices_from_sitemaps(
    sitemap_dir: Path,
    location_type: LocationType,
) -> set[int] | None:
    sitemap_type = f"{location_type.lower()}s"
    sitemap_subdir = Path(sitemap_dir, sitemap_type)
    all_known_indices: set[int] = set()
    if not sitemap_subdir.is_dir():
        return None
    for xml_path in (pbar := tqdm(list(sitemap_subdir.glob("*.xml")))):
        pbar.set_description(str(xml_path))
        tree = ET.parse(xml_path)
        namespace = {"": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for url in tree.getroot().findall(".//url", namespace):
            loc_element = url.find("loc", namespace)
            if loc_element is None or not isinstance(loc_element.text, str):
                continue
            loc_text = loc_element.text.strip()
            match = re.search(rf"{location_type}%5E(\d+)", loc_text)
            if match:
                all_known_indices.add(int(match.group(1)))
    return all_known_indices


class ResultDict(TypedDict):
    identifier: str
    name: str
    area: str
    index: int
    type: LocationType
    url: str
    url_api: str


def get_chunk_range_from_string(range_str: str) -> tuple[int, int]:
    search = re.search(r"(\d+)_(\d+)", range_str)
    if search:
        return int(search.group(1)), int(search.group(2))
    raise ValueError(f"Couldn't get range from {range_str}")


@dataclass
class RightmoveLocationScraper:
    output_dir: Path
    location_type: LocationType
    min_seconds_between_requests: float | None = 1
    use_api = False
    channel = Channel.RENT
    all_known_indices: set[int] | None = None
    sitemap_dir: Path | None = None
    query = {
        "sort_type": 4,
        "radius": 40.0,
    }

    def __post_init__(self) -> None:
        if self.sitemap_dir and self.location_type is LocationType.STATION:
            # currently only supports station sitemap, which has identifiers
            # TODO: problem: some stations may now be present on the website but not in the sitemaps (e.g. Abbey Wood)
            known_indices = get_indices_from_sitemaps(self.sitemap_dir, self.location_type)
            if known_indices:
                self.all_known_indices = known_indices

    def get_one(self, i: int):
        if self.use_api:
            return self.get_one_api(i)
        else:
            return self.get_one_scrape(i)

    @property
    def location_filepath(self) -> Path:
        return Path(self.output_dir, f"{self.location_type}-all").with_suffix(".json")

    def get_and_write_all(
        self,
        start_index: int | None = None,
        end_index: int | float | None = None,
    ) -> None:

        results = []
        if start_index is None:
            start_index = 0
            if self.location_filepath.is_file():
                results = read_list_from_file(self.location_filepath)
                if results:
                    latest_index = max(e["index"] for e in results)
                    start_index = latest_index + 1

        if end_index is None or isinf(end_index):
            if end_index is None and self.all_known_indices:
                iterator = sorted(list(self.all_known_indices))
                iterator = [i for i in iterator if i >= start_index]
            else:
                iterator = count(start_index)

        for current_index in (pbar := tqdm(iterator)):
            pbar.set_description(self.get_identifier(current_index))
            result = self.get_one(current_index)
            results.append(result)
            dump_data(results, self.location_filepath)

    def get_url_api(
        self,
        identifier: str,
    ) -> str:
        return make_api_url(identifier, self.channel, **self.query)

    def get_url_scrape(
        self,
        identifier: str,
    ) -> str:
        return make_scrape_url(identifier, self.channel, **self.query)

    def get_identifier(self, location_index: int) -> str:
        return f"{self.location_type}^{location_index}"

    def get_one_scrape(
        self,
        location_index: int,
    ) -> ResultDict | None:
        identifier = self.get_identifier(location_index)

        url = self.get_url_scrape(identifier)
        try:
            html = make_get_request_to_url(url, min_delay=self.min_seconds_between_requests)
        except HTTPError as exc:
            if exc.response.status_code in {404}:
                return None
            raise
        soup = BeautifulSoup(html, "html.parser")

        result: ResultDict = {
            "identifier": identifier,
            "name": get_name_from_soup(soup),
            "area": get_area_from_soup(soup),
            "type": self.location_type,
            "index": location_index,
            "url": url,
            "url_api": self.get_url_api(identifier),
        }
        return result

    def get_one_api(
        self,
        location_index: int,
    ) -> ResultDict | None:
        identifier = self.get_identifier(location_index)
        url = self.get_url_api(identifier)
        try:
            res = make_get_request_to_url(
                url,
                min_delay=self.min_seconds_between_requests,
                format="json",
            )
        except HTTPError as exc:
            if exc.response.status_code in {404}:
                return None
            raise
        retrieved_result = res["location"]
        result: ResultDict = {
            "identifier": identifier,
            "name": retrieved_result["shortDisplayName"],
            "area": get_area_from_text(
                retrieved_result["displayName"],
                retrieved_result["shortDisplayName"],
            ),
            "type": retrieved_result["locationType"],
            "index": retrieved_result["id"],
            "url": self.get_url_scrape(identifier),
            "url_api": url,
        }
        return result
