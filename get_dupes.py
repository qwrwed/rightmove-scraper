from argparse import ArgumentParser, Namespace
from collections import defaultdict
from pathlib import Path

from utils_python import dump_data, make_parent_dir, read_dict_from_file

from constants import DEFAULT_JSON_DUPES_DIR, DEFAULT_JSON_FULL_DIR


class ArgsNamespace(Namespace):
    input_dir: Path
    output_dir: Path


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-i", "--input-dir", type=Path, default=DEFAULT_JSON_FULL_DIR)
    parser.add_argument("-o", "--output-dir", type=Path, default=DEFAULT_JSON_DUPES_DIR)
    return parser.parse_args(namespace=ArgsNamespace())


def get_dupes_from_file(filepath: Path):
    identifier_name_dict = read_dict_from_file(filepath)
    name_identifiers_dict = defaultdict(set)
    for k, v in identifier_name_dict.items():
        name_identifiers_dict[v].add(k)
    return {k: v for k, v in name_identifiers_dict.items() if len(v) > 1}


def get_and_write_dupes(input_dir: Path, output_dir: Path):
    for filepath in input_dir.glob("*-identifier_to_name*.json"):
        dupes = get_dupes_from_file(filepath)
        if not dupes:
            continue
        outfile = Path(output_dir, filepath.with_stem(f"{filepath.stem}-dupes").name)
        make_parent_dir(outfile)
        dump_data(dupes, outfile)


def main():
    args = parse_args()
    get_and_write_dupes(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
