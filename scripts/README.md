# Scripts Folder


This folder contains the scripts used to scrape the DHQ website and compile the articles into a dataset. The scripts are as follows:

1. `dhq_repo_observer.py`: This script checks if the DHQ repository has been updated since the last time the articles were downloaded. If changes exist, it redownloads the articles that exist in the `data/dhq-journal` directory. You will need to have the GitHub API Tokens set as environment variables for this script to work.