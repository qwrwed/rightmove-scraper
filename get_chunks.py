import logging
import re
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm
from utils_python import (
    dump_data,
    make_get_request_to_url,
    make_parent_dir,
    setup_tqdm_logger,
)

from constants import DEFAULT_JSON_CHUNK_DIR, KNOWN_IDENTIFIER_TYPES

LOGGER = logging.getLogger(__name__)


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
        choices=KNOWN_IDENTIFIER_TYPES,
    )
    parser.add_argument("-c", "--chunk-size", type=int, default=100)
    parser.add_argument("-o", "--output-dir", type=Path, default=DEFAULT_JSON_CHUNK_DIR)
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


def get_name(identifier: str, min_delay: float | None = None):
    url = f"https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier={identifier}"
    html = make_get_request_to_url(url, delay=min_delay)
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    input_soup = soup.find_all("input", {"class": "input--full"})
    if len(input_soup) != 1:
        raise NotImplementedError(
            f"Unexpected number of inputs (expected 1): {input_soup!r}"
        )
    [input_element] = input_soup
    input_value = input_element.get("value")

    return input_value


MAP_DIRECTIONS = {"name_to_identifier", "identifier_to_name"}


def getChunk(
    identifier_type: str, i_start: int, i_end: int, min_delay: float | None = None
):
    LOGGER.info(f"Getting {identifier_type} [{i_start}, {i_end})")
    maps: dict[str, dict[str, str]] = {k: {} for k in MAP_DIRECTIONS}

    for i in (pbar := tqdm(range(i_start, i_end))):
        identifier = f"{identifier_type}^{i}"
        pbar.set_description(identifier)
        name = get_name(identifier, min_delay)
        if name:
            LOGGER.debug("%s -> %s", identifier, name)
            maps["identifier_to_name"][identifier] = name
            maps["name_to_identifier"][name] = identifier

    if not any(maps.values()):
        return None

    return maps


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
        make_path = lambda map_direction: Path(
            identifier_type, map_direction, range_str
        ).with_suffix(".json")

        LOGGER.info(
            "Writing %i values to '%s'", len(list(chunk.values())[0]), make_path("*")
        )
        for map_direction, map_contents in chunk.items():
            filepath = Path(output_dir, make_path(map_direction))
            make_parent_dir(filepath)
            dump_data(map_contents, filepath)

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
    files_in_directions = defaultdict(set)
    for map_direction_dir in identifier_type_dir.iterdir():
        if not map_direction_dir.is_dir():
            continue
        for path in map_direction_dir.glob("*.json"):
            name = path.name
            files_in_directions[map_direction_dir.name].add(name)
            all_files.add(name)
    if len(files_in_directions) < len(MAP_DIRECTIONS):
        # at least one of the directions has no files
        return 0
    elif (found_directions := set(files_in_directions.keys())) != MAP_DIRECTIONS:
        raise ValueError(f"{found_directions=} doesn't match known {MAP_DIRECTIONS=}")
    missing_files = all_files - set.intersection(*files_in_directions.values())
    if missing_files:
        earliest_missing_filename = min(missing_files)
        chunk_start, chunk_end = get_chunk_range(earliest_missing_filename)
        return chunk_start
    else:
        latest_found_filename = max(all_files)
        chunk_start, chunk_end = get_chunk_range(latest_found_filename)
        return chunk_end + 1


def main():
    setup_tqdm_logger(level=logging.INFO)
    args = parse_args()
    getAndWriteAll(
        args.output_dir, args.identifier_type, args.start, args.end, args.chunk_size, args.rate_limit
    )


if __name__ == "__main__":
    main()
