from argparse import ArgumentParser, Namespace
from pathlib import Path


class BaseArgsNamespace(Namespace):
    app_config_path: Path


def add_base_args(
    parser: ArgumentParser | None = None,
) -> ArgumentParser:
    parser = parser or ArgumentParser()
    parser.add_argument(
        "-c",
        "--app-config-path",
        nargs="?",
        type=Path,
        default=Path("config/app.yaml"),
    )
    return parser


def get_base_args() -> BaseArgsNamespace:
    parser = add_base_args()
    return parser.parse_args(namespace=BaseArgsNamespace())
