# Scripts Folder


This folder contains the scripts used to scrape the DHQ website and compile the articles into a dataset. The scripts are as follows:

1. `dhq_repo_observer.py`: This script checks if the DHQ repository has been updated since the last time the articles were downloaded. If changes exist, it redownloads the articles that exist in the `data/dhq-journal` directory. You will need to have the GitHub API Tokens set as environment variables for this script to work.
2. `utils.py`: This script contains utility functions used by the other scripts. Specifically, this script contains functions for processing the XML files into structured dataset.
3. `dhq_website_scraper.py`: This script also scrapes the DHQ website to get relevant issue metadata for each article. Any website or scraping code is in this script.
4. `process_dhq_articles.py`: This is the final script, and it cleans and combines the data from the XML files and the website scraping into a single dataset. It also infers some missing data based on issues.