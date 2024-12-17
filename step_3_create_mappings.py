import logging
from pathlib import Path

from utils_python import dump_data, read_dict_from_file, setup_tqdm_logger

from base_args import get_base_args
from rightmove_scraper.config import Config
from rightmove_scraper.location_scraper import ResultDict

LOGGER = logging.getLogger(__name__)


def create_mappings(
    entries: dict[str, ResultDict],
    key: str,
) -> dict[str | int, list[ResultDict]]:
    mappings: dict[str | int, list[ResultDict]] = {}
    for entry in entries.values():
        if entry is not None:
            mappings.setdefault(entry[key], []).append(entry)
    return mappings


def write_mappings_from_file(
    input_filepath: Path,
    output_dir: Path,
    key: str,
) -> None:

    output_path = Path(
        output_dir,
        input_filepath.with_stem(f"{input_filepath.stem}-mappings-by-{key}").name,
    )
    if output_path == input_filepath:
        print(f"{output_path=} == {input_filepath=}, returning")

    entries = read_dict_from_file(input_filepath)
    mappings = create_mappings(entries, key)

    print(f"Writing to '{output_path}'")
    dump_data(mappings, output_path)


if __name__ == "__main__":
    setup_tqdm_logger(level=logging.INFO)
    _args = get_base_args()
    _config = Config.from_file(_args.app_config_path)
    for _input_filepath in _config.location.dir.glob("*.json"):
        write_mappings_from_file(
            _input_filepath,
            output_dir=_config.mappings.dir,
            key=_config.mappings.key,
        )
