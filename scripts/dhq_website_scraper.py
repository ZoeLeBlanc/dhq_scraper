import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import sys
import os
import re
from typing import Tuple
from utils import process_xml_files, generate_xml_files

def check_if_link_exists(link: str) -> bool:
    """ 
    Function to check if a link exists
    Args:
        link (str): Link to check
    Returns:
        bool: True if the link exists, False otherwise
    """
    # Check if the link exists
    response = requests.get(link)
    # If the link exists and is not Resource Found Error, return True
    if (response.status_code == 200) and ("Resource Not Found" not in response.text):
        return True
    else:
        return False

def process_article_links(issue_links_df: pd.DataFrame) -> pd.DataFrame:
    """ 
    Function to scrape the article links from the issue links dataframe
    Args:
        issue_links_df (pd.DataFrame): Dataframe containing the issue links
    Returns:
        pd.DataFrame: Dataframe containing the scraped article links
    """
    # If the CSV file exists, read it into a DataFrame 
    if os.path.exists("../data/dhq_article_links.csv"):
        article_links_df = pd.read_csv("../data/dhq_article_links.csv")
    else:
        # DataFrame to store extracted data
        article_links_dfs = []
        progress_bar = tqdm(total=len(issue_links_df), desc='Scraping articles')
        for _, row in issue_links_df.iterrows():
            # Check if the issue link exists
            issue_response = requests.get(row.issue_link)
            progress_bar.update(1)
            if issue_response.status_code == 200:
                issue_soup = BeautifulSoup(issue_response.text, "html.parser")
                toc = issue_soup.find("div", {"id": "toc"})
                issue_title = toc.find_all('h2')[0].get_text()
                editors = toc.find_all('h3')

                editors = editors[0].get_text() if len(editors) > 0 else ''
                articles = toc.find_all('div', {'class': 'articleInfo'})
                for article in articles:
                    authors = article.find_all('div')[0].get_text()
                    article_link = "http://www.digitalhumanities.org" + article.find('a').get('href')
                    article_text = article.find('a').get_text()
                    abstract = article.find('span', {'class': 'viewAbstract'})
                    abstract = abstract.get_text() if abstract else ''
                    data = {'issue_title': issue_title, 'editors': editors, 'authors': authors, 'article_link': article_link, 'article_title': article_text, 'abstract': abstract, 'issue_text': row.issue_text, "issue_link": row.issue_link}
                    article_links_dfs.append(data)
        progress_bar.close()
        article_links_df = pd.DataFrame(article_links_dfs)
        pattern = r'.*vol/([^/]*)/([^/]*)/(\d+)/\3\.html'
        article_links_df[['volume', 'issue', 'DHQarticle-id']] = article_links_df.article_link.str.extract(pattern)
        article_links_df['DHQarticle-id'] = article_links_df['DHQarticle-id'].astype(str)
        article_links_df.to_csv("../data/dhq_article_links.csv", index=False)
    return article_links_df

def download_xml_links(article_links_df: pd.DataFrame, missing_directory: str) -> pd.DataFrame:
    """
    Download XML files from provided article links and extract volume, issue, and DHQarticle-id from the link.

    Parameters:
    - article_links_df: DataFrame containing article links.

    Returns:
    - DataFrame with columns ['xml_link', 'volume', 'issue', 'DHQarticle-id'].
    """
    save_path = missing_directory
    # Ensure the save directory exists
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    # DataFrame to store extracted data
    xml_data = []

    article_progress_bar = tqdm(total=len(article_links_df), desc='Scraping articles')

    for _, row in article_links_df.iterrows():
        article_progress_bar.update(1)

        article_response = requests.get(row.article_link)

        if article_response.status_code == 200:
            article_soup = BeautifulSoup(article_response.text, "html.parser")
            xml_links = article_soup.find_all('a')
            xml_links = [link.get('href') for link in xml_links if link.get('href') and 'xml' in link.get('href')]
            # Download xml
            for link in xml_links:
                # Extract the required components from the link
                match = re.search(r'/dhq/vol/(\d+)/(\d+)/(\d+)\.xml', link)
                if match:
                    volume, issue, dhq_id = match.groups()

                    base_url = "http://digitalhumanities.org:8081"
                    absolute_link = base_url + link
                    xml_response = requests.get(absolute_link)


                    # If request is successful, save XML to file
                    if xml_response.status_code == 200:
                        # Save the XML content to a file
                        file_name = f"{dhq_id}.xml"
                        file_path = os.path.join(save_path, file_name)
                        with open(file_path, 'wb') as file:
                            file.write(xml_response.content)
                        
                        # Save link and extracted data to the xml_data list
                        xml_data.append({'xml_link': link, 'volume': volume, 'issue': issue, 'DHQarticle-id': dhq_id})
                    else:
                        print(f"Failed to download {link}")
        else:
            print(f"Failed to scrape {row.article_link}")

    article_progress_bar.close()

    # Convert the xml_data list to a DataFrame
    xml_df = pd.DataFrame(xml_data)

    return xml_df

def process_issue_links(issue_links: list) -> pd.DataFrame:
    """
    Function to scrape the issue links from the issue links list
    Args:
        issue_links (list): List containing the issue links
    Returns:
        pd.DataFrame: Dataframe containing the scraped issue links
    """
    # If the CSV file exists, read it into a DataFrame
    if os.path.exists("../data/dhq_issue_links.csv"):
        issue_links_df = pd.read_csv("../data/dhq_issue_links.csv")
    else:
        # List to store the issue links
        issue_links_dfs = []
        for link in issue_links:
            if ('vol' in link.get('href')) or ('preview' in link.get('href')):
                issue_links_dfs.append({'issue_link': "http://www.digitalhumanities.org" +link.get('href'), 'issue_text': link.get_text()})
        issue_links_df = pd.DataFrame(issue_links_dfs)
        issue_links_df.to_csv("../data/dhq_issue_links.csv", index=False)
    return issue_links_df

def scrape_dhq(existing_articles_df: pd.DataFrame, missing_directory: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Function to scrape the DHQ website and download missing articles.
    
    Args:
        existing_articles_df (pd.DataFrame): DataFrame of existing articles.
        missing_directory (str): Directory to save the missing data.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Tuple containing two DataFrames. The first DataFrame contains the links to all articles. The second DataFrame contains the scraped data of the missing articles.
    """
    try:
        # URL of the DHQ website
        url = "http://www.digitalhumanities.org/dhq/"

        # Send a GET request to the DHQ website
        response = requests.get(url)

        # Parse the response text with BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the div with id "leftsidenav" that contains the issue links
        div = soup.find("div", {"id": "leftsidenav"})

        # Find all 'a' elements (links) in the div
        issue_links = div.find_all('a')

        # Process the issue links to extract the required data
        issue_links_df = process_issue_links(issue_links)

        # Process the article links to extract the required data
        article_links_df = process_article_links(issue_links_df)

        # Find the articles that are missing from the existing articles
        missing_articles = article_links_df[~article_links_df['DHQarticle-id'].isin(existing_articles_df['DHQarticle-id'])]

        print(f"Missing articles: {len(missing_articles)}")

        # If there are missing articles, download them
        if len(missing_articles) > 0:
            print("Downloading missing articles...")

            # Download the XML links of the missing articles
            xml_links_df = download_xml_links(missing_articles, missing_directory)

            # Save the XML links to a CSV file
            xml_links_df.to_csv("../data/missing_dhq_xml_links.csv", index=False)

            # Generate the XML files of the missing articles
            updated_xml_files = generate_xml_files(missing_directory)

            # Process the XML files to extract the required data
            updated_df = process_xml_files(updated_xml_files, missing_directory)

            # Save the scraped data to a CSV file
            updated_df.to_csv("../data/missing_dhq_data.csv", index=False)
        else:
            # If there are no missing articles, create an empty DataFrame
            updated_df = pd.DataFrame()

        return article_links_df, updated_df
    
    except requests.exceptions.RequestException as e:
        # Handle exceptions raised by the requests library
        print(e)
        sys.exit(1)
    except Exception as e:
        # Handle all other exceptions
        print(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    existing_articles_df = pd.read_csv("../data/initial_dhq_data.csv")
    missing_directory = "../data/missing_dhq_data"
    scrape_dhq(existing_articles_df, missing_directory)