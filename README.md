# DHQ Scraper

This repository compiles the most recent articles from *Digital Humanities Quarterly* (DHQ) into a dataset, along with relevant metadata.

## Repository Structure

The repository is structured as follows:

### Scripts

1. `dhq_repo_observer.py`: This script checks if the DHQ repository has been updated since the last time the articles were downloaded. If changes exist, it redownloads the articles that exist in the `data/dhq-journal` directory. You will need to have the GitHub API Tokens set as environment variables for this script to work.
2. `utils.py`: This script contains utility functions used by the other scripts. Specifically, this script contains functions for processing the XML files into structured dataset.
3. `dhq_website_scraper.py`: This script also scrapes the DHQ website to get relevant issue metadata for each article. Any website or scraping code is in this script.
4. `process_dhq_articles.py`: This is the final script, and it cleans and combines the data from the XML files and the website scraping into a single dataset. It also infers some missing data based on issues.

### Data

1. `data/dhq-journal`: This directory contains the cloned repository of [https://github.com/Digital-Humanities-Quarterly/dhq-journal/](https://github.com/Digital-Humanities-Quarterly/dhq-journal/).
2. `dhq_issue_links.csv`: This file contains the links to the issues of DHQ. This is used by the `dhq_website_scraper.py` script.
3. `dhq_articles_links.csv`: This file contains the links to individual articles of DHQ. This is used by the `dhq_website_scraper.py` script.
4. `initial_dhq_data.csv`: This file contains the data from the XML files. This is used by the `process_dhq_articles.py` script.
5. `processed_dhq_data.csv`: This file contains the final dataset. This is used by the `process_dhq_articles.py` script.

### Notebooks

1. `DHQEDA.ipybn`: This notebook contains the exploratory data analysis of the dataset.
