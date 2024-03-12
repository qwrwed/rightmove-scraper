import logging
import re
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm
from utils_python import dump_data, make_get_request_to_url, setup_tqdm_logger

from constants import DEFAULT_DATA_ROOT, IDENTIFIER_TYPES, SEARCH_URL
from utils import get_area_from_soup, get_name_from_soup

LOGGER = logging.getLogger(__name__)
DEFAULT_CHUNK_DIR = Path(DEFAULT_DATA_ROOT, "json_chunks")


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
        args.start = get_next_chunk(args.output_dir, args.identifier_type)
    return args


def get_identifier_url(identifier: str):
    return SEARCH_URL.format(identifier)


def getChunk(
    identifier_type: str, i_start: int, i_end: int, min_delay: float | None = None
):
    LOGGER.info(f"Getting {identifier_type} [{i_start}, {i_end})")
    results = []
    for i in (pbar := tqdm(range(i_start, i_end))):
        identifier = f"{identifier_type}^{i}"
        pbar.set_description(identifier)

        url = get_identifier_url(identifier)
        html = make_get_request_to_url(url, delay=min_delay)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")

        result = {
            "identifier": identifier,
            "identifier_parts": {
                "type": identifier_type,
                "index": i,
            },
            "url": url,
            "name": get_name_from_soup(soup),
            "area": get_area_from_soup(soup),
        }

        results.append(result)
    return results


def getAndWriteAll(
    output_dir: Path,
    identifier_type: str,
    start=0,
    end=None,
    chunk_size=100,
    min_delay: float | None = None,
):
    chunk_start = start
    while chunk_start < (end or float("inf")):
        chunk_end = chunk_start + chunk_size
        chunk = getChunk(identifier_type, chunk_start, chunk_end, min_delay)
        if not chunk:
            break
        range_str = f"{chunk_start:08}_{chunk_end-1:08}"
        filepath = Path(output_dir, identifier_type, range_str).with_suffix(".json")
        LOGGER.info("Writing %i values to '%s'", len(chunk), filepath)

        dump_data(chunk, filepath)

        chunk_start = chunk_end


def get_chunk_range(range_str: str):
    search = re.search(r"(\d+)_(\d+)", range_str)
    if search:
        return (int(d) for d in search.groups())
    return None, None


def get_next_chunk(output_dir: Path, identifier_type: str):
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


def main():
    setup_tqdm_logger(level=logging.INFO)
    args = parse_args()
    getAndWriteAll(
        args.output_dir,
        args.identifier_type,
        args.start,
        args.end,
        args.chunk_size,
        args.rate_limit,
    )


if __name__ == "__main__":
    main()
