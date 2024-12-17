# rightmove-scraper

Generates mappings/dicts of Rightmove identifiers to the strings they represent.

## Usage
1. Retrieve sitemaps
    - ```bash
      python ./step_1_get_sitemaps.py
      ```
2. Get results for each location identifier:
    - ```bash
      python ./step_2_get_locations.py
      ```
    - Script will resume from latest identifier if interrupted.
3. Generate mappings from location file:
    - ```bash
      python ./step_3_create_mappings.py
      ```
