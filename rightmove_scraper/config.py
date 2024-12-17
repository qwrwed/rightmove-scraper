from io import IOBase
from pathlib import Path
from pprint import pprint
from typing import Any, Self

from pydantic import BaseModel, model_validator
from pydantic_yaml import parse_yaml_file_as

from rightmove_scraper.location_scraper import LocationType, RightmoveLocationScraper
from rightmove_scraper.sitemap_scraper import (
    DEFAULT_ROOT_SITEMAP_URL,
    RightmoveSitemapScraper,
    SitemapType,
)


class FileModel(BaseModel):
    @classmethod
    def from_file(cls, file: Path | str | IOBase) -> Self:
        return parse_yaml_file_as(cls, file)


class SubConfig(FileModel):
    @model_validator(mode="before")
    @classmethod
    def compute_absolute_path(cls, values: dict[str, Any]) -> dict[str, Any]:
        if values["_parent_dir"] and values["relative_dir"]:
            values["dir"] = Path(values["_parent_dir"], values["relative_dir"])
        return values


class SitemapConfig(SubConfig):
    dir: Path
    types: list[SitemapType]
    overwrite: bool = False
    root_url: str = DEFAULT_ROOT_SITEMAP_URL


class LocationConfig(SubConfig):
    dir: Path
    location_type: LocationType
    min_seconds_between_requests: float = 1.0
    use_api: bool = False


class MappingsConfig(SubConfig):
    dir: Path
    key: str = "name"


class Config(FileModel):
    data_dir: Path
    sitemap: SitemapConfig
    location: LocationConfig
    mappings: MappingsConfig

    @model_validator(mode="before")
    @classmethod
    def set_subconfig_main_path(cls, values: dict[str, Any]) -> dict[str, Any]:
        for subconfig_key in ["sitemap", "location", "mappings"]:
            if values["data_dir"] and values[subconfig_key]:
                values[subconfig_key]["_parent_dir"] = values["data_dir"]
        return values


def make_rightmove_sitemap_scraper(config: SitemapConfig) -> RightmoveSitemapScraper:
    return RightmoveSitemapScraper(
        sitemap_dir=config.dir,
        types=config.types,
        overwrite=config.overwrite,
    )


def make_rightmove_location_scraper(
    config: LocationConfig,
    sitemap_dir: Path | None = None,
) -> RightmoveLocationScraper:
    return RightmoveLocationScraper(
        output_dir=config.dir,
        location_type=config.location_type,
        min_seconds_between_requests=config.min_seconds_between_requests,
        sitemap_dir=sitemap_dir,
    )


if __name__ == "__main__":
    pprint(Config.from_file("config/app.yaml").model_dump(), sort_dicts=False)
