
import pandas as pd
from tqdm import tqdm
from utils import process_xml_files, generate_xml_files, get_scraped_dhq_files
import os

def check_duplicates(rows):
    if len(rows)> 1:
        rows = rows.sort_values(by=['date_when'], ascending=False)
        rows = rows[0:1]
    return rows

def infer_dates(rows):
    missing_dates = rows[rows.date_processed.isna()]
    has_dates = rows[rows.date_processed.notna()]
    latest_date = has_dates.date_processed.max()
    missing_dates.date_processed = latest_date
    rows = pd.concat([missing_dates, has_dates])
    return rows

def create_dataset(directory_path):
    xml_files = generate_xml_files(directory_path)
    df = process_xml_files(xml_files, "../data/initial_dhq_data.csv")
    df.date_when = df.date_when.str.replace('Feburary', 'February')
    df['date_processed'] = pd.to_datetime(df['date_when'])
    tqdm.pandas(desc="Processing dates")
    df = df.groupby('DHQarticle-id').progress_apply(check_duplicates)
    df.loc[df.file_name == "../data/dhq_data/000664.xml", "volume"] = "016"
    df.loc[df.file_name == "../data/dhq_data/000664.xml", "issue"] = "4"
    df.loc[df.volume.isna(), 'volume'] = 'yet to be assigned'
    df.loc[df.issue.isna(), 'issue'] = 'yet to be assigned'
    tqdm.pandas(desc="Inferring dates")
    df = df.groupby(['volume', 'issue']).progress_apply(infer_dates)
    df.to_csv("../data/initial_dhq_data.csv", index=False)
    df = df.reset_index(drop=True)
    article_links_df, updated_df = get_scraped_dhq_files()
    if len(updated_df) > 0:
        df = pd.concat([df, updated_df])
        df = df.reset_index(drop=True)
    if len(article_links_df) > 0:
        article_links_df = article_links_df.rename(columns={'authors': 'scraped_authors', 'editors': 'scraped_editors'})
        df['volume'] = df['volume'].astype(str)
        df['issue'] = df['issue'].astype(str)
        df['DHQarticle-id'] = df['DHQarticle-id'].astype(str)
        article_links_df['volume'] = article_links_df['volume'].astype(str)
        article_links_df['issue'] = article_links_df['issue'].astype(str)
        article_links_df['DHQarticle-id'] = article_links_df['DHQarticle-id'].astype(str)
        merged_df = pd.merge(df, article_links_df, on=['DHQarticle-id', 'volume', 'issue'], how='left')
        merged_df.to_csv("../data/processed_dhq_data.csv", index=False)

if __name__ == "__main__":
    create_dataset("../data/dhq-journal/articles")