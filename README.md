# rightmove-scraper

Generates mappings/dicts of Rightmove identifiers to the strings they represent.

## Usage
1. Generate mappings, split into chunks:
    - ```bash
      python ./get_chunks.py --type REGION
      ```
    - `--type` can be any of `{STATION, REGION, POSTCODE, OUTCODE}`
    - Script will resume from latest chunk if interrupted.
2. Combine chunks into a single file:
    - ```bash
      python ./combine_chunks.py
      ```
Output JSON files each contain a dict of either:
- name to identifier
- identifier to name

The filename will include which of the above it is.

---

TODO: handle multiple identifiers with the same name:
- Currently `name_to_identifier` will have newer identifiers overwrite old identifiers
- TODO: write a script which updates all `name_to_identifier` files in a folder to have a list of all identifiers which correspond to a name if there are multiple
- For now, duplicates can be identified with:
    - ```bash
        python ./get_dupes.py
        ```