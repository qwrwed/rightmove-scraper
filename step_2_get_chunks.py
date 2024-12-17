import logging
from argparse import ArgumentParser
from pprint import pformat

from utils_python import setup_tqdm_logger

from base_args import BaseArgsNamespace, add_base_args
from rightmove_scraper.config import (
    Config,
    make_rightmove_location_scraper,
    make_rightmove_sitemap_scraper,
)

LOGGER = logging.getLogger(__name__)


def get_and_write_all(
    config: Config,
    start_index: int | None = None,
    end_index: int | None = None,
) -> None:

    rightmove_sitemap_scraper = make_rightmove_sitemap_scraper(config.sitemap)
    rightmove_sitemap_scraper.get_and_download_sitemaps()

    rightmove_location_scraper = make_rightmove_location_scraper(
        config.chunks,
        rightmove_sitemap_scraper.sitemap_dir,
    )
    rightmove_location_scraper.get_and_write_all(start_index, end_index)


class ArgsNamespace(BaseArgsNamespace):
    start_index: int | None
    end_index: int | None


def parse_args() -> ArgsNamespace:
    parser = ArgumentParser()
    add_base_args(parser)
    parser.add_argument("-s", "--start-index", type=int, default=None,)
    parser.add_argument("-e", "--end-index", type=int, default=None,)
    args = parser.parse_args(namespace=ArgsNamespace())
    return args


if __name__ == "__main__":
    setup_tqdm_logger(level=logging.INFO)
    _args = parse_args()
    _config = Config.from_file(_args.app_config_path)
    LOGGER.info("loaded config:\n%s", pformat(_config.model_dump()))
    get_and_write_all(
        _config,
        _args.start_index,
        _args.end_index,
    )
