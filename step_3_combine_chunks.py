import logging
from collections import defaultdict
from pathlib import Path
from pprint import pformat
from typing import Any

from tqdm import tqdm
from utils_python import dump_data, read_list_from_file, setup_tqdm_logger

from base_args import get_base_args
from rightmove_scraper.config import Config

LOGGER = logging.getLogger(__name__)


def get_file_paths(input_dir: Path) -> defaultdict[str, list[Path]]:
    # retrieve file paths
    all_files: defaultdict[str, list[Path]] = defaultdict(list)
    for p in input_dir.rglob("*.json"):
        identifier_type, _map_range = p.relative_to(input_dir).parts
        all_files[identifier_type].append(p)

    # sort retrieved files' paths by name
    for direction_files in all_files.values():
        direction_files.sort()

    return all_files


def combine_file_contents(files: list[Path]) -> list[Any]:
    combined_contents = []
    for file in tqdm(files):
        combined_contents.extend(read_list_from_file(file))
    return combined_contents


def main() -> None:
    # TODO: have this run step 1
    setup_tqdm_logger(level=logging.INFO)
    _args = get_base_args()
    _config = Config.from_file(_args.app_config_path)
    LOGGER.info("loaded config:\n%s", pformat(_config.model_dump()))

    all_files = get_file_paths(_config.chunks.dir)

    for identifier_type, direction_files in all_files.items():
        LOGGER.info(
            "Combining %i files for %s",
            len(direction_files),
            identifier_type,
        )
        combined_data = combine_file_contents(direction_files)

        combined_file_path = Path(_config.chunks_combined.dir, f"{identifier_type}-all.json")
        LOGGER.info("Saving to %s", combined_file_path)
        dump_data(combined_data, combined_file_path)
    LOGGER.info("Done")


if __name__ == "__main__":
    main()
