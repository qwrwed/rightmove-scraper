from pathlib import Path

SEARCH_URL = (
    "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier={}"
)

AREA_PATTERN = r"Properties (?:To Rent|For Sale) (?:in|near) {}, (.*?)(:?, within .*)?$"

IDENTIFIER_TYPES = {
    "STATION",
    "REGION",
    "OUTCODE",
    "POSTCODE",
}
IDENTIFIER_SEP = "^"

DEFAULT_DATA_ROOT = Path("./data")
