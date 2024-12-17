from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Sequence
from urllib.parse import urlsplit

from lxml import etree as ET
from tqdm import tqdm
from utils_python import dump_data, make_get_request_to_url

if TYPE_CHECKING:
    from lxml.etree import _Element

LOGGER = logging.getLogger(__name__)


def url_to_filename(url: str) -> str:
    return urlsplit(url).path.strip("/")


SitemapsByCategory = dict[str, dict[str, Path]]


class SitemapType(Enum):
    PROPERTIES = "properties"
    STATIONS = "stations"
    STATIC = "static"
    OVERSEAS = "overseas"
    AGENTS = "agents"
    OUTCODES = "outcodes"
    REGIONS = "regions"


DEFAULT_ROOT_SITEMAP_URL = "https://www.rightmove.co.uk/sitemap.xml"


@dataclass
class RightmoveSitemapScraper:
    sitemap_dir: Path
    types: list[SitemapType]
    overwrite: bool = False
    root_xml_tree: _Element | None = None

    def get_root_sitemap(
        self,
        root_sitemap_url: str = DEFAULT_ROOT_SITEMAP_URL,
        save: bool = True,
    ) -> _Element:
        root_sitemap_path = Path(self.sitemap_dir, url_to_filename(root_sitemap_url))
        if root_sitemap_path.is_file() and not self.overwrite:
            LOGGER.info(f"Root sitemap '{root_sitemap_path}' already downloaded")
            with open(root_sitemap_path, "rb") as f:
                root_sitemap_bytes = f.read()
        else:
            LOGGER.info(f"Downloading '{root_sitemap_url}' -> '{root_sitemap_path}'")
            root_sitemap_bytes = make_get_request_to_url(root_sitemap_url, format="bytes")
            if save:
                dump_data(root_sitemap_bytes, root_sitemap_path, "wb")
        self.root_xml_tree = ET.fromstring(root_sitemap_bytes)
        return self.root_xml_tree

    def get_sitemaps_to_download(
        self,
        sitemap_types: Sequence[SitemapType],
    ) -> SitemapsByCategory:
        if not self.root_xml_tree:
            self.get_root_sitemap()
            assert self.root_xml_tree is not None

        sitemaps: SitemapsByCategory = {}
        LOGGER.info("Finding sitemaps...")
        elements = self.root_xml_tree.findall(
            f".//{{{next(iter(self.root_xml_tree.nsmap.values()))}}}loc",
        )
        for sitemap_tag in (pbar := tqdm(elements, leave=False)):
            sitemap_url = sitemap_tag.text
            if not sitemap_url:
                raise ValueError(f"Got null {sitemap_url=} from {sitemap_tag=}")
            pbar.set_description(sitemap_url)

            sitemap_name = url_to_filename(sitemap_url)
            match = re.match(r"^sitemap-(\w+)-(.+).xml$", sitemap_name)
            if match is None:
                LOGGER.warning(f"Unexpected filename format {sitemap_name}")
                continue
            category, _value = match.groups()
            if (sitemap_type := SitemapType(category)) not in sitemap_types:
                LOGGER.debug(f"Skipping unwanted '{sitemap_type.value}' sitemap '{sitemap_name}'")
                continue

            sitemap_path = Path(self.sitemap_dir, category, sitemap_name)
            if sitemap_path.is_file() and not self.overwrite:
                LOGGER.debug(f"Skipping existing sitemap '{sitemap_path}'")
                continue

            category_sitemaps = sitemaps.setdefault(category, {})
            if sitemap_url in category_sitemaps:
                LOGGER.debug(f"Skipping duplicate url '{sitemap_url}'")
                continue

            LOGGER.debug(f"Found sitemap '{sitemap_url}' -> '{sitemap_path}'")
            category_sitemaps[sitemap_url] = sitemap_path

        sitemap_summary = {category: len(category_sitemaps) for category, category_sitemaps in sitemaps.items()}
        if sitemaps:
            LOGGER.info(f"Found sitemaps to download: {sitemap_summary}")
        else:
            LOGGER.info("No sitemaps to download.")
        return sitemaps

    @staticmethod
    def download_sitemaps(sitemaps: SitemapsByCategory) -> None:
        if not sitemaps:
            return
        LOGGER.info("Downloading sitemaps...")
        for category, sitemap_urls in (pbar1 := tqdm(sitemaps.items(), leave=False)):
            pbar1.set_description(category)
            for sitemap_url, sitemap_path in (pbar2 := tqdm(sitemap_urls.items(), leave=False)):
                pbar2.set_description(sitemap_url)
                LOGGER.info(f"Downloading '{sitemap_url}' to '{sitemap_path}'")
                sitemap_bytes = make_get_request_to_url(sitemap_url, format="bytes")
                dump_data(sitemap_bytes, sitemap_path, "wb")
        LOGGER.info("Downloaded sitemaps.")

    def get_and_download_sitemaps(self) -> None:
        sitemaps_to_download = self.get_sitemaps_to_download(self.types)
        self.download_sitemaps(sitemaps_to_download)
