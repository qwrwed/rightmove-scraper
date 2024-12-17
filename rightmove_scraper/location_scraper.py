import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, TypedDict
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from requests import HTTPError
from tqdm import tqdm
from utils_python import dump_data, make_get_request_to_url, read_list_from_file,print_tqdm

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
    chunk_size: int = 100
    sitemap_dir: Path | None = None
    query = {
        "sort_type": 4,
        "radius": 40.0,
    }

    def __post_init__(self) -> None:
        if self.sitemap_dir and self.location_type is LocationType.STATION:
            # currently only supports station sitemap, which has identifiers
            known_indices = get_indices_from_sitemaps(self.sitemap_dir, self.location_type)
            if known_indices:
                self.all_known_indices = known_indices

    def get_filtered_indices(
        self,
        i_start: int,
        i_end: int,
    ) -> Iterable[int]:
        chunk_indices: Iterable[int] = range(i_start, i_end)
        if self.all_known_indices is not None:
            chunk_indices = [i for i in chunk_indices if i in self.all_known_indices]
        return chunk_indices

    def get_next_to_scrape(self) -> tuple[int, int]:
        location_type_dir = Path(self.output_dir, self.location_type)
        if not location_type_dir.is_dir():
            # folder doesn't exist
            return 0, 0
        all_files = set()
        for path in location_type_dir.glob("*.json"):
            all_files.add(path.name)
        if not all_files:
            # folder is empty
            return 0, 0

        latest_found_filename = max(all_files)
        contents = read_list_from_file(Path(location_type_dir, latest_found_filename))
        latest_index = max(e["index"] for e in contents)

        chunk_start, chunk_end = get_chunk_range_from_string(latest_found_filename)
        chunk_indices = self.get_filtered_indices(chunk_start, chunk_end)
        remaining_indices = [i for i in chunk_indices if i > latest_index]

        if remaining_indices:
            next_chunk_start_index, next_scrape_index = chunk_start, min(remaining_indices)
        else:
            next_chunk_start_index, next_scrape_index = chunk_end + 1, chunk_end + 1
        return next_chunk_start_index, next_scrape_index

    def get_and_write_chunk(
        self,
        i_start: int,
        i_end: int,
        scrape_start: int | None = None,
    ) -> None:
        if scrape_start is None:
            scrape_start = i_start
        chunk = []
        chunk_indices = self.get_filtered_indices(scrape_start, i_end)

        range_str = f"{i_start:08}_{i_end-1:08}"
        filepath = Path(self.output_dir, self.location_type, range_str).with_suffix(".json")

        chunk = read_list_from_file(filepath)

        for i in tqdm(chunk_indices, desc=f"{i_start}-{i_end-1}"):
            if self.use_api:
                result = self.get_one_api(i)
            else:
                result = self.get_one_scrape(i)
            if result:
                chunk.append(result)
                dump_data(chunk, filepath)
        LOGGER.info("Wrote %i values to '%s'", len(chunk), filepath)

    def get_and_write_all(
        self,
        start_index: int | None = None,
        end_index: int | float | None = None,
    ) -> None:
        
        if start_index is None:
            chunk_start, scrape_start = self.get_next_to_scrape()
        else:
            chunk_start = scrape_start = start_index
        if end_index is None:
            if self.all_known_indices is not None:
                end_index = max(self.all_known_indices)
            else:
                end_index = float("inf")
        assert end_index is not None  # mypy
        while chunk_start < end_index:
            chunk_end = chunk_start + self.chunk_size
            self.get_and_write_chunk(chunk_start, chunk_end, scrape_start)
            scrape_start = chunk_start = chunk_end

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

    def get_one_scrape(
        self,
        location_index: int,
    ) -> ResultDict | None:
        identifier = f"{self.location_type}^{location_index}"

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
        identifier = f"{self.location_type}^{location_index}"
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
