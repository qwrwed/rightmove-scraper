# rightmove-scraper

Generates mappings/dicts of Rightmove identifiers to the strings they represent.

## Usage
1. Generate results, split into chunks:
    - ```bash
      python ./get_chunks.py --type REGION
      ```
    - `--type` can be any of `{STATION, REGION, POSTCODE, OUTCODE}`
    - Script will resume from latest chunk if interrupted.
2. Combine chunks into single files:
    - ```bash
      python ./combine_chunks.py
      ```
3. Generate mappings from combined files:
    - ```bash
      python ./create_mappings.py
      ```
    - This will generate :
      - one file of mappings for found names which correspond to one identifier
      - one file of mappings for found names which correspond to multiple identifiers
