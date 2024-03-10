import logging
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from pathlib import Path

from tqdm import tqdm
from utils_python import (
    dump_data,
    make_parent_dir,
    read_dict_from_file,
    setup_root_logger,
)

from constants import DEFAULT_JSON_CHUNK_DIR, DEFAULT_JSON_FULL_DIR

LOGGER = logging.getLogger(__name__)


class ArgsNamespace(Namespace):
    input_dir: Path
    output_dir: Path


def get_args():
    parser = ArgumentParser()
    parser.add_argument(
        "-i",
        "--input-dir",
        default=DEFAULT_JSON_CHUNK_DIR,
        help="default: '%(default)s'",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=DEFAULT_JSON_FULL_DIR,
        help="default: '%(default)s'",
    )
    args = parser.parse_args(namespace=ArgsNamespace())
    if not args.output_dir:
        args.output_dir = args.input_dir
    return args


def get_files(input_dir: Path):
    # retrieve file paths
    all_files: defaultdict[tuple[str, str], list[Path]] = defaultdict(list)
    for p in input_dir.rglob("*.json"):
        identifier_type, map_direction, _map_range = p.relative_to(input_dir).parts
        all_files[(identifier_type, map_direction)].append(p)

    # sort retrieved file paths
    for direction_files in all_files.values():
        direction_files.sort()

    return all_files


def combine_file_dicts(files: list[Path]):
    combined_dict = {}
    for file in tqdm(files):
        combined_dict.update(read_dict_from_file(file))
    return combined_dict


def main():
    setup_root_logger()
    args = get_args()
    all_files = get_files(args.input_dir)

    for (map_type, map_direction), direction_files in all_files.items():
        LOGGER.info(
            "Combining %i files for (%s, %s)",
            len(direction_files),
            map_type,
            map_direction,
        )
        combined_data = combine_file_dicts(direction_files)

        combined_file_path = Path(
            args.output_dir, f"{map_type}-{map_direction}-all.json"
        )
        LOGGER.info("Saving to %s", combined_file_path)
        make_parent_dir(combined_file_path)
        dump_data(combined_data, combined_file_path)
    LOGGER.info("Done")


if __name__ == "__main__":
    main()
