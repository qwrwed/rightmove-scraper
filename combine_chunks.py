import logging
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from pathlib import Path

from tqdm import tqdm
from utils_python import dump_data, read_list_from_file, setup_root_logger

from constants import DEFAULT_DATA_ROOT
from get_chunks import DEFAULT_CHUNK_DIR

LOGGER = logging.getLogger(__name__)
DEFAULT_COMBINED_DIR = Path(DEFAULT_DATA_ROOT, "json_combined")


class ArgsNamespace(Namespace):
    input_dir: Path
    output_dir: Path


def get_args():
    parser = ArgumentParser()
    parser.add_argument(
        "-i",
        "--input-dir",
        default=DEFAULT_CHUNK_DIR,
        help="default: '%(default)s'",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=DEFAULT_COMBINED_DIR,
        help="default: '%(default)s'",
    )
    return parser.parse_args(namespace=ArgsNamespace())


def get_file_paths(input_dir: Path):
    # retrieve file paths
    all_files: defaultdict[str, list[Path]] = defaultdict(list)
    for p in input_dir.rglob("*.json"):
        identifier_type, _map_range = p.relative_to(input_dir).parts
        all_files[identifier_type].append(p)

    # sort retrieved files' paths by name
    for direction_files in all_files.values():
        direction_files.sort()

    return all_files


def combine_file_contents(files: list[Path]):
    combined_contents = []
    for file in tqdm(files):
        combined_contents.extend(read_list_from_file(file))
    return combined_contents


def main():
    setup_root_logger()
    args = get_args()
    all_files = get_file_paths(args.input_dir)

    for identifier_type, direction_files in all_files.items():
        LOGGER.info(
            "Combining %i files for %s",
            len(direction_files),
            identifier_type,
        )
        combined_data = combine_file_contents(direction_files)

        combined_file_path = Path(args.output_dir, f"{identifier_type}-all.json")
        LOGGER.info("Saving to %s", combined_file_path)
        dump_data(combined_data, combined_file_path)
    LOGGER.info("Done")


if __name__ == "__main__":
    main()
