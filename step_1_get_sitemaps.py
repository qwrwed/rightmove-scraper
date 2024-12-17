import logging
from pprint import pformat

from utils_python import setup_tqdm_logger

from base_args import get_base_args
from rightmove_scraper.config import Config, make_rightmove_sitemap_scraper

LOGGER = logging.getLogger(__name__)


if __name__ == "__main__":
    args = get_base_args()
    setup_tqdm_logger(level=logging.INFO)
    _config = Config.from_file(args.app_config_path)
    LOGGER.info("loaded config:\n%s", pformat(_config.model_dump()))
    make_rightmove_sitemap_scraper(_config.sitemap).get_and_download_sitemaps()
