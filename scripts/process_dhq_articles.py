import pandas as pd
from typing import List, Any
from tqdm import tqdm
from utils import process_xml_files, generate_xml_files, get_scraped_dhq_files
from dhq_website_scraper import check_if_link_exists
import os

def check_duplicates(rows: pd.DataFrame) -> pd.DataFrame:
    """
    Function to check for duplicate rows based on the 'date_when' column.
    If duplicates are found, it keeps only the row with the latest date.

    Args:
        rows (pd.DataFrame): DataFrame to check for duplicates.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    # If there is more than one row, sort them by 'date_when' in descending order
    if len(rows) > 1:
        rows = rows.sort_values(by=['date_when'], ascending=False)
        # Keep only the first row (the one with the latest date)
        rows = rows[0:1]
    return rows

def infer_dates(rows: pd.DataFrame) -> pd.DataFrame:
    """
    Function to infer missing dates in the 'date_processed' column.
    It assigns the maximum date to the rows with missing dates.

    Args:
        rows (pd.DataFrame): DataFrame to infer missing dates.

    Returns:
        pd.DataFrame: DataFrame with missing dates inferred.
    """
    # Separate the rows with missing dates and the rows with dates
    missing_dates = rows[rows.date_processed.isna()]
    has_dates = rows[rows.date_processed.notna()]

    # Find the latest date
    latest_date = has_dates.date_processed.max()

    # Assign the latest date to the rows with missing dates
    missing_dates.date_processed = latest_date

    # Concatenate the rows with missing dates and the rows with dates
    rows = pd.concat([missing_dates, has_dates])
    return rows

def create_dataset(directory_path: str, processed_df_output_path: str) -> pd.DataFrame:
    """
    Function to create a dataset from XML files in a given directory.
    The dataset is saved to a CSV file.

    Args:
        directory_path (str): Path to the directory containing the XML files.
        processed_df_output_path (str): Path to the output CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the processed data.
    """
    # If the output file already exists, load it into a DataFrame
    if os.path.exists(processed_df_output_path):
        processed_df = pd.read_csv(processed_df_output_path)
    else:
        # Generate a list of XML files in the directory
        xml_files = generate_xml_files(directory_path)

        # Process the XML files and save the data to a DataFrame
        df = process_xml_files(xml_files, "../data/initial_dhq_data.csv")

        # Correct a typo in the 'date_when' column
        df.date_when = df.date_when.str.replace('Feburary', 'February')

        # Convert the 'date_when' column to datetime format
        df['date_processed'] = pd.to_datetime(df['date_when'])

        # Apply the 'check_duplicates' function to each group of rows with the same 'DHQarticle-id'
        df = df.groupby('DHQarticle-id', group_keys=False).progress_apply(check_duplicates)

        # Correct the 'volume' and 'issue' values for a specific file
        df.loc[df.file_name == "../data/dhq_data/000664.xml", "volume"] = "016"
        df.loc[df.file_name == "../data/dhq_data/000664.xml", "issue"] = "4"

        # Assign a default value to the missing 'volume' and 'issue' values
        df.loc[df.volume.isna(), 'volume'] = 'yet to be assigned'
        df.loc[df.issue.isna(), 'issue'] = 'yet to be assigned'

        # Apply the 'infer_dates' function to each group of rows with the same 'volume' and 'issue'
        df = df.groupby(['volume', 'issue'], group_keys=False).progress_apply(infer_dates)

        # Save the DataFrame to a CSV file
        df.to_csv("../data/initial_dhq_data.csv", index=False)

        # Reset the index of the DataFrame
        df = df.reset_index(drop=True)

        # Get the scraped DHQ files
        article_links_df, updated_df = get_scraped_dhq_files()

        # If there are updated files, concatenate them to the DataFrame
        if len(updated_df) > 0:
            df = pd.concat([df, updated_df])
            df = df.reset_index(drop=True)

        # If there are article links, merge them with the DataFrame
        if len(article_links_df) > 0:
            article_links_df = article_links_df.rename(columns={'authors': 'scraped_authors', 'editors': 'scraped_editors'})
            df['volume'] = df['volume'].astype(str)
            df['issue'] = df['issue'].astype(str)
            df['DHQarticle-id'] = df['DHQarticle-id'].astype(str)
            article_links_df['volume'] = article_links_df['volume'].astype(str)
            article_links_df['issue'] = article_links_df['issue'].astype(str)
            article_links_df['DHQarticle-id'] = article_links_df['DHQarticle-id'].astype(str)
            processed_df = pd.merge(df, article_links_df, on=['DHQarticle-id', 'volume', 'issue'], how='left')
            processed_df.to_csv(processed_df_output_path, index=False)
        else:
            print("No article links found")
            processed_df = df

    return processed_df

def create_article_link(row: pd.Series) -> pd.Series:
    """
    Function to create an article link for a given row if it doesn't exist.
    
    Args:
        row (pd.Series): A row of the DataFrame.

    Returns:
        pd.Series: The row with the created article link.
    """
    # If the article link is missing, create it
    if pd.isna(row.article_link):
        volume = row.volume
        issue = row.issue
        article_id = row['DHQarticle-id']
        base_url = "http://digitalhumanities.org:8081"
        article_link = f"{base_url}/dhq/vol/{volume}/{issue}/{article_id}/{article_id}.html"

        # If the created link exists, assign it to the 'article_link' column
        if check_if_link_exists(article_link):
            row['article_link'] = article_link

    return row

def infer_issue_data(rows: pd.DataFrame) -> pd.DataFrame:
    """
    Function to infer issue data for a given group of rows.
    
    Args:
        rows (pd.DataFrame): A group of rows of the DataFrame.

    Returns:
        pd.DataFrame: The group of rows with the inferred issue data.
    """
    # List of columns to infer
    columns = ['issue_title', 'scraped_editors', 'issue_text', 'issue_link']

    # For each column, assign the first unique non-null value to all rows
    for col in columns:
        unique_values = rows[col].dropna().unique()
        rows[col] = unique_values[0] if unique_values.size > 0 else None

    return rows

def finalize_dataset(processed_df: pd.DataFrame) -> None:
    """
    Function to finalize the dataset by creating missing article links and inferring issue data.
    The finalized dataset is saved to a CSV file.

    Args:
        processed_df (pd.DataFrame): The processed DataFrame.
    """
    # Create missing article links
    tqdm.pandas(desc="Creating article links")
    processed_df = processed_df.progress_apply(create_article_link, axis=1, result_type="expand")

    # Infer issue data by grouping by 'volume' and 'issue'
    processed_df = processed_df.groupby(['volume', 'issue'], group_keys=False).progress_apply(infer_issue_data)

    # Save the finalized dataset to a CSV file
    processed_df.to_csv("../data/processed_dhq_data.csv", index=False)


if __name__ == "__main__":
    processed_df = create_dataset("../data/dhq-journal/articles", "../data/processed_dhq_data.csv")
    processed_df['DHQarticle-id'] = processed_df['DHQarticle-id'].astype(str)
    processed_df['DHQarticle-id'] = processed_df['DHQarticle-id'].str.zfill(6)
    finalize_dataset(processed_df)