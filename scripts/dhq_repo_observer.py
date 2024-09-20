import requests
import os
import subprocess
from typing import Optional

import apikey

# Load the GitHub personal access token
auth_token: str = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

# Set up the headers for the GitHub API request
auth_headers: dict = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

# GitHub Repository details
REPO_OWNER: str = "Digital-Humanities-Quarterly"
REPO_NAME: str = "dhq-journal"
FOLDER_PATH: str = "articles"  # Use full path from repo root
LOCAL_REPO_PATH: str = "../data/dhq-journal"
API_URL: str = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/commits?path={FOLDER_PATH}&per_page=1"

def get_latest_commit_sha(query: str) -> Optional[str]:
    """
    Function to get the latest commit SHA using the GitHub API
    Args:
        query (str): The API URL to fetch the latest commit
    Returns:
        str: The latest commit SHA if successful, None otherwise
    """
    response = requests.get(query, headers=auth_headers, timeout=5)
    if response.status_code == 200:
        latest_commit_sha = response.json()[0]['sha']
        return latest_commit_sha
    else:
        print("Failed to fetch data from GitHub API")
        return None

def get_latest_local_commit_sha() -> Optional[str]:
    """
    Function to get the latest commit SHA from the local Git repository
    Returns:
        str: The latest commit SHA if successful, None otherwise
    """
    if os.path.exists(LOCAL_REPO_PATH):
        try:
            # Run the Git command to get the latest commit SHA from the specified repo folder
            latest_local_commit_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=LOCAL_REPO_PATH
            ).strip().decode()
            return latest_local_commit_sha
        except subprocess.CalledProcessError as e:
            print("An error occurred while accessing the local Git repository.")
            return None
    else:
        print("Local repository does not exist.")
        return None

def compare_commits() -> None:
    """
    Function to compare the latest commit SHA from the GitHub API with the latest known local commit SHA.
    If they are different, it means there are new updates in the repository.
    In this case, the function will pull the updates if the local repository exists, or clone the repository if it doesn't.
    If the commit SHAs are the same, it means there are no new updates in the repository.
    """
    # Get the latest commit SHA from the GitHub API
    latest_remote_commit_sha: str = get_latest_commit_sha(API_URL)
    
    # Get the latest known local commit SHA
    last_local_known_commit: str = get_latest_local_commit_sha()

    # Compare the remote and local commit SHAs
    if latest_remote_commit_sha != last_local_known_commit:
        print("New updates found. Cloning or pulling the repository...")

        # If the local repository exists, pull the updates
        if os.path.exists(LOCAL_REPO_PATH):
            subprocess.run(["git", "pull"], cwd=LOCAL_REPO_PATH)
        # If the local repository doesn't exist, clone the repository
        else:
            subprocess.run(["git", "clone", f"git@github.com:{REPO_OWNER}/{REPO_NAME}.git", LOCAL_REPO_PATH])
    # If the remote and local commit SHAs are the same, there are no new updates
    else:
        print("No new updates in the repository.")

if __name__ == "__main__":
    # Call the compare_commits function when the script is run directly
    compare_commits()