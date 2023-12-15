import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import sys
import os
import re

def process_articles(article_links_df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to scrape the articles from the article links dataframe
    Args:
        article_links_df (pd.DataFrame): Dataframe containing the article links
    Returns:
        pd.DataFrame: Dataframe containing the scraped articles
    """
    dfs = []
    article_progress_bar = tqdm(total=len(article_links_df), desc='Scraping articles')
    for _, row in article_links_df.iterrows():
        article_progress_bar.update(1)
        article_response = requests.get(row.article_link)
        if article_response.status_code == 200:
            article_soup = BeautifulSoup(article_response.text, "html.parser")
            article = article_soup.find('div', {'class': 'DHQarticle'})
            pubinfo = article.find('div', {'id': 'pubInfo'})
            header = article.find('div', {'class': 'DHQheader'})
            header_lang = header.find('h1').get('class')
            header_lang = header_lang[0] if len(header_lang) > 0 else ''
            header_title = header.find('h1').get_text()
            authors = header.find_all('div', {'class': 'author'})
            article_text = article.find('div', {'id': 'DHQtext'})
            article_abstract = article_text.find('div', {'id': 'abstract'})
            article_abstract = article_abstract.get_text() if article_abstract is not None else '' 
            article_text = article_text.find_all('div', {'class': 'div div0'})
            extracted_text = ''
            for text in article_text:
                extracted_text += text.get_text()
            notes = article.find('div', {'id': 'notes'})
            notes = notes.find_all('div', {'class': 'endnote'}) if notes else []
            works_cited = article.find('div', {'id': 'worksCited'})
            works_cited = works_cited.find_all('div', {'class': 'bibl'}) if works_cited else []
            final_row = row.to_dict()
            final_row['article_text'] = [article_text]
            final_row['article_abstract'] = article_abstract
            final_row['extracted_text'] = extracted_text
            final_row['header_lang'] = header_lang
            final_row['header_title'] = header_title
            final_row['authors'] = authors
            final_row['notes'] = [notes]
            final_row['works_cited'] = [works_cited]
            final_row['pubinfo'] = pubinfo
            dfs.append(final_row)
    article_progress_bar.close()
    df = pd.DataFrame(dfs)
    return df

def process_article_links(issue_links_df: pd.DataFrame) -> pd.DataFrame:
    """ 
    Function to scrape the article links from the issue links dataframe
    Args:
        issue_links_df (pd.DataFrame): Dataframe containing the issue links
    Returns:
        pd.DataFrame: Dataframe containing the scraped article links
    """
    if os.path.exists("../data/dhq_article_links.csv"):
        article_links_df = pd.read_csv("../data/dhq_article_links.csv")
    else:
        article_links_dfs = []
        progress_bar = tqdm(total=len(issue_links_df), desc='Scraping articles')
        for _, row in issue_links_df.iterrows():
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
        article_links_df.to_csv("../data/dhq_article_links.csv", index=False)
    return article_links_df

def download_xml_links(article_links_df: pd.DataFrame) -> pd.DataFrame:
    """
    Download XML files from provided article links and extract volume, issue, and DHQarticle-id from the link.

    Parameters:
    - article_links_df: DataFrame containing article links.

    Returns:
    - DataFrame with columns ['xml_link', 'volume', 'issue', 'DHQarticle-id'].
    """
    
    save_path = "../data/dhq_data/"
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
    if os.path.exists("../data/dhq_issue_links.csv"):
        issue_links_df = pd.read_csv("../data/dhq_issue_links.csv")
    else:
        issue_links_dfs = []
        for link in issue_links:
            if ('vol' in link.get('href')) or ('preview' in link.get('href')):
                issue_links_dfs.append({'issue_link': "http://www.digitalhumanities.org" +link.get('href'), 'issue_text': link.get_text()})
        issue_links_df = pd.DataFrame(issue_links_dfs)
        issue_links_df.to_csv("../data/dhq_issue_links.csv", index=False)
    return issue_links_df

def scrape_dhq(output_path: str):
    """
    Function to scrape the DHQ website
    Args:
        output_path (str): Path to save the scraped data
    """
    try:
        url = "http://www.digitalhumanities.org/dhq/"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        div = soup.find("div", {"id": "leftsidenav"})
        issue_links = div.find_all('a')
        issue_links_df = process_issue_links(issue_links)
        article_links_df = process_article_links(issue_links_df)
        #get xml files
        xml_links_df = download_xml_links(article_links_df)
        xml_links_df.to_csv(output_path, index=False)
        # article_df = process_articles(article_links_df)
        # article_df.to_csv(output_path, index=False)
    except requests.exceptions.RequestException as e:
        print(e)
        sys.exit(1)

if __name__ == "__main__":
    output_path = "../data/dhq_xml_links.csv"
    scrape_dhq(output_path)