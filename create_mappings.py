import logging
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from pathlib import Path

from utils_python import dump_data, read_list_from_file, serialize_data

from combine_chunks import DEFAULT_COMBINED_DIR
from utils import DEFAULT_DATA_ROOT

LOGGER = logging.getLogger(__name__)
DEFAULT_MAPPING_DIR = Path(DEFAULT_DATA_ROOT, "json_mappings")


class ArgsNamespace(Namespace):
    input_dir: Path
    output_dir: Path


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        "-i",
        "--input-dir",
        default=DEFAULT_COMBINED_DIR,
        help="default: '%(default)s'",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=DEFAULT_MAPPING_DIR,
        help="default: '%(default)s'",
    )
    return parser.parse_args(namespace=ArgsNamespace())


def do_file(filepath: Path):
    things = read_list_from_file(filepath)

    mappings = defaultdict(list)
    for thing in things:
        mappings[thing["name"]].append(thing)

    mappings_single = {}
    mappings_multiple = {}
    for name, name_mappings in mappings.items():
        if len(name_mappings) == 1:
            mappings_single[name] = name_mappings
        else:
            mappings_multiple[name] = name_mappings

    return mappings_single, mappings_multiple


def do_files(input_dir: Path, output_dir: Path):
    for filepath in input_dir.glob("*.json"):
        mappings_single, mappings_multiple = do_file(filepath)
        if mappings_single:
            outfile = Path(
                output_dir, filepath.with_stem(f"{filepath.stem}-mappings-single").name
            )
            dump_data(mappings_single, outfile)
        if mappings_multiple:
            outfile = Path(
                output_dir,
                filepath.with_stem(f"{filepath.stem}-mappings-multiple").name,
            )
            dump_data(mappings_multiple, outfile)


def main():
    args = parse_args()
    do_files(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
