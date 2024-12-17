# rightmove-scraper

Generates mappings/dicts of Rightmove identifiers to the strings they represent.

## Usage
1. Retrieve sitemaps
    - ```bash
      python ./step_1_get_sitemaps.py
      ```
2. Generate results, split into chunks:
    - ```bash
      python ./step_2_get_sitemaps.py
      ```
    - Script will resume from latest chunk if interrupted.
3. Combine chunks into single files:
    - ```bash
      python ./step_3_combine_chunks.py
      ```
4. Generate mappings from combined files:
    - ```bash
      python ./step_4_create_mappings.py
      ```
