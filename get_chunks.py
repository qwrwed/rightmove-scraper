import logging
import re
import xml.etree.ElementTree as ET
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from pathlib import Path

from bs4 import BeautifulSoup
from requests import HTTPError
from tqdm import tqdm
from utils_python import (
    dump_data,
    make_get_request_to_url,
    serialize_data,
    setup_tqdm_logger,
)

from utils import (
    DEFAULT_DATA_ROOT,
    get_area_from_soup,
    get_area_from_text,
    get_name_from_soup,
)

LOGGER = logging.getLogger(__name__)
DEFAULT_CHUNK_DIR = Path(DEFAULT_DATA_ROOT, "json_chunks")
IDENTIFIER_TYPES = {
    "STATION",
    "REGION",
    "OUTCODE",
    "POSTCODE",
}


class ArgsNamespace(Namespace):
    start: int
    end: int | None
    identifier_type: str
    chunk_size: int
    output_dir: Path
    min_delay: float | None


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-s", "--start", type=int, default=None)
    parser.add_argument("-e", "--end", type=int, default=None)
    parser.add_argument(
        "-t",
        "--type",
        dest="identifier_type",
        default="STATION",
        choices=IDENTIFIER_TYPES,
    )
    parser.add_argument("-c", "--chunk-size", type=int, default=100)
    parser.add_argument("-o", "--output-dir", type=Path, default=DEFAULT_CHUNK_DIR)
    parser.add_argument(
        "-r",
        "--rate-limit",
        metavar="MIN_DELAY",
        help="minimum time between requests",
        type=float,
    )
    args = parser.parse_args(namespace=ArgsNamespace())
    if args.start is None:
        args.start = get_next_chunk_to_scrape(args.output_dir, args.identifier_type)
    return args


PROPERTY_CHANNELS = {"RENT": "property-to-rent", "BUY": "property-for-sale"}
DEFAULT_CHANNEL = "RENT"


def get_chunk_range(range_str: str):
    search = re.search(r"(\d+)_(\d+)", range_str)
    if search:
        return (int(d) for d in search.groups())
    return None, None


def get_next_chunk_to_scrape(output_dir: Path, identifier_type: str):
    identifier_type_dir = Path(output_dir, identifier_type)
    if not identifier_type_dir.is_dir():
        # folder doesn't exist
        return 0
    all_files = set()
    for path in identifier_type_dir.glob("*.json"):
        all_files.add(path.name)
    latest_found_filename = max(all_files)
    chunk_start, chunk_end = get_chunk_range(latest_found_filename)
    return chunk_end + 1


@dataclass
class RightmoveScraper:
    output_dir: Path
    location_type: str
    min_delay: float | None = 1
    use_api = True
    channel = DEFAULT_CHANNEL
    all_known_indices: set[str] | None = None

    def __post_init__(self):
        assert self.channel in PROPERTY_CHANNELS
        assert self.location_type in IDENTIFIER_TYPES
        if self.location_type == "STATION":
            xml_path = Path("xml/sitemap-stations-ALL.xml")
            if not xml_path.is_file():
                return
            tree = ET.parse(xml_path)
            namespace = {"": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            self.all_known_indices = set()
            for url in tree.getroot().findall(".//url", namespace):
                loc_element = url.find("loc", namespace)
                if loc_element is None:
                    continue
                loc_text = loc_element.text.strip()
                self.all_known_indices.add(
                    int(re.search(r"STATION%5E(\d+)", loc_text).group(1))
                )
            print(len(self.all_known_indices))

    def getAndWriteAll(
        self,
        start=0,
        end=None,
        chunk_size=100,
    ):
        chunk_start = start
        if end is None:
            if self.all_known_indices is not None:
                end = max(self.all_known_indices)
            else:
                end = float("inf")
        while chunk_start < end:
            chunk_end = chunk_start + chunk_size
            chunk = self.getChunk(chunk_start, chunk_end)
            if chunk:
                range_str = f"{chunk_start:08}_{chunk_end-1:08}"
                filepath = Path(
                    self.output_dir, self.location_type, range_str
                ).with_suffix(".json")
                LOGGER.info("Writing %i values to '%s'", len(chunk), filepath)
                dump_data(chunk, filepath)
            chunk_start = chunk_end

    def getChunk(
        self,
        i_start: int,
        i_end: int,
    ):
        results = []
        for i in tqdm(range(i_start, i_end), desc=f"{i_start}-{i_end-1}"):
            if self.all_known_indices and i not in self.all_known_indices:
                continue
            if self.use_api:
                result = self.getOneAPI(i)
            else:
                result = self.getOneScrape(i)
            if result:
                results.append(result)
        return results

    def getUrlAPI(self, identifier):
        return f"https://www.rightmove.co.uk/api/_search?locationIdentifier={identifier}&channel={self.channel}"

    def getUrlScrape(self, identifier):
        return f"https://www.rightmove.co.uk/{PROPERTY_CHANNELS[self.channel]}/find.html?locationIdentifier={identifier}"

    def getOneScrape(self, location_index: int):
        identifier = f"{self.location_type}^{location_index}"

        url = self.getUrlScrape(identifier)
        try:
            html = make_get_request_to_url(url, min_delay=self.min_delay)
        except HTTPError as exc:
            if exc.response.status_code in {404}:
                return None
        soup = BeautifulSoup(html, "html.parser")

        result = {
            "identifier": identifier,
            "name": get_name_from_soup(soup),
            "area": get_area_from_soup(soup),
            "type": self.location_type,
            "index": location_index,
            "url": url,
            "url_api": self.getUrlAPI(identifier),
        }
        return result

    def getOneAPI(self, location_index: int):
        identifier = f"{self.location_type}^{location_index}"
        url = self.getUrlAPI(identifier)
        try:
            res = make_get_request_to_url(url, min_delay=self.min_delay, format="json")
        except HTTPError as exc:
            if exc.response.status_code in {404}:
                return None
            raise
        retrieved_result = res["location"]
        result = {
            "identifier": identifier,
            "name": retrieved_result["shortDisplayName"],
            "area": get_area_from_text(
                retrieved_result["displayName"],
                retrieved_result["shortDisplayName"],
            ),
            "type": retrieved_result["locationType"],
            "index": retrieved_result["id"],
            "url": self.getUrlScrape(identifier),
            "url_api": url,
        }
        return result


def main():
    setup_tqdm_logger(level=logging.INFO)
    args = parse_args()
    rightmove_scraper = RightmoveScraper(
        args.output_dir,
        args.identifier_type,
        args.rate_limit,
    )
    rightmove_scraper.getAndWriteAll(
        args.start,
        args.end,
        args.chunk_size,
    )


if __name__ == "__main__":
    main()
