import requests
import os
import subprocess
import apikey

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}
# GitHub Repository details
REPO_OWNER = "Digital-Humanities-Quarterly"
REPO_NAME = "dhq-journal"
FOLDER_PATH = "articles"  # Use full path from repo root
LOCAL_REPO_PATH = "../data/dhq-journal"
API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/commits?path={FOLDER_PATH}&per_page=1"


# Function to get the latest commit SHA using the GitHub API
def get_latest_commit_sha(query):
    response = requests.get(query, headers=auth_headers, timeout=5)
    if response.status_code == 200:
        latest_commit_sha = response.json()[0]['sha']
        return latest_commit_sha
    else:
        print("Failed to fetch data from GitHub API")
        return None

def get_latest_local_commit_sha():
    if os.path.exists(LOCAL_REPO_PATH):
        os.chdir(LOCAL_REPO_PATH)
        try:
            # Run the Git command to get the latest commit SHA
            latest_local_commit_sha = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip().decode()
            return latest_local_commit_sha
        except subprocess.CalledProcessError as e:
            print("An error occurred while accessing the local Git repository.")
            return None
    else:
        print("Local repository does not exist.")
        return None


# Get the latest commit SHA from GitHub API
latest_remote_commit_sha = get_latest_commit_sha(API_URL)
last_local_known_commit = get_latest_local_commit_sha()

# Compare and act accordingly
if latest_remote_commit_sha != last_local_known_commit:
    print("New updates found. Cloning or pulling the repository...")

    # Clone or pull the repository
    if os.path.exists(LOCAL_REPO_PATH):
        os.chdir(LOCAL_REPO_PATH)
        subprocess.run(["git", "pull"])
    else:
        subprocess.run(["git", "clone", f"git@github.com:{REPO_OWNER}/{REPO_NAME}.git", LOCAL_REPO_PATH])

else:
    print("No new updates in the repository.")

