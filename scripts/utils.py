import xml.etree.ElementTree as ET
import pandas as pd
import os
from tqdm import tqdm

def process_xml_files(xml_files, output_path):
    # Define the XML namespaces
    namespaces = {
        'tei': "http://www.tei-c.org/ns/1.0",
        'dhq': "http://www.digitalhumanities.org/ns/dhq",
        'xml': "http://www.w3.org/XML/1998/namespace"
    }
    if os.path.exists(output_path):
        final_df = pd.read_csv(output_path)
    else:
        all_data = []
        for file_name in tqdm(xml_files, desc="Processing XML files"):
        
            with open(file_name, 'r', encoding='utf-8') as f:
                file_content = f.read().strip()
                
            if file_content == '<?xml version="1.0" encoding="UTF-8"?>':
                continue  # Skip to the next file if the content is just the XML declaration
            
            try:
                tree = ET.parse(file_name)
                root = tree.getroot()
            except ET.ParseError:
                print(f"Error parsing {file_name}. Skipping...")
                continue

            # Extracting the required data with checks to avoid errors
            base_data = {
                'DHQarticle-id': root.find(".//tei:publicationStmt/tei:idno[@type='DHQarticle-id']", namespaces=namespaces).text if root.find(".//tei:publicationStmt/tei:idno[@type='DHQarticle-id']", namespaces=namespaces) is not None else None,
                'volume': root.find(".//tei:publicationStmt/tei:idno[@type='volume']", namespaces=namespaces).text if root.find(".//tei:publicationStmt/tei:idno[@type='volume']", namespaces=namespaces) is not None else None,
                'issue': root.find(".//tei:publicationStmt/tei:idno[@type='issue']", namespaces=namespaces).text if root.find(".//tei:publicationStmt/tei:idno[@type='issue']", namespaces=namespaces) is not None else None,
                'articleType': root.find(".//tei:publicationStmt/dhq:articleType", namespaces=namespaces).text if root.find(".//tei:publicationStmt/dhq:articleType", namespaces=namespaces) is not None else None,
                'date_when': root.find(".//tei:publicationStmt/tei:date", namespaces=namespaces).text if root.find(".//tei:publicationStmt/tei:date", namespaces=namespaces) is not None else None,
                'dhq_keywords': root.find(".//tei:encodingDesc/tei:classDecl/tei:taxonomy[@xml:id='dhq_keywords']/tei:bibl", namespaces=namespaces).text if root.find(".//tei:encodingDesc/tei:classDecl/tei:taxonomy[@xml:id='dhq_keywords']/tei:bibl", namespaces=namespaces) is not None else None,
                'language_ident': root.find(".//tei:profileDesc/tei:langUsage/tei:language", namespaces=namespaces).attrib['ident'] if root.find(".//tei:profileDesc/tei:langUsage/tei:language", namespaces=namespaces) is not None else None,
                'dhq_abstract': root.find(".//tei:text/tei:front/dhq:abstract/tei:p", namespaces=namespaces).text if root.find(".//tei:text/tei:front/dhq:abstract/tei:p", namespaces=namespaces) is not None else None,
                'file_name': file_name
            }

            # Extract title
            title_element = root.find(".//tei:titleStmt/tei:title", namespaces=namespaces)
            if title_element is not None:
                # Concatenate all text and tail components of the element and its descendants
                title_parts = [title_element.text] + [e.text + (e.tail if e.tail else "") for e in title_element.findall(".//")]
                base_data['title'] = "".join(filter(None, title_parts))

            # Extract author information
            author_elements = root.findall(".//tei:titleStmt/dhq:authorInfo", namespaces=namespaces)

            authors_data = []

            for author_element in author_elements:
                author_data = {}
                
                # Extract author name
                author_name_element = author_element.find("dhq:author_name", namespaces=namespaces)
                if author_name_element is not None:
                    first_name = author_name_element.text
                    last_name_element = author_name_element.find("dhq:family", namespaces=namespaces)
                    if last_name_element is not None:
                        full_name = f"{first_name} {last_name_element.text}".strip()
                    else:
                        full_name = first_name
                    author_data['author_name'] = full_name

                # Extract affiliation
                affiliation_element = author_element.find("dhq:affiliation", namespaces=namespaces)
                if affiliation_element is not None:
                    author_data['affiliation'] = affiliation_element.text

                # Extract email
                email_element = author_element.find("email", namespaces=namespaces)
                if email_element is not None:
                    author_data['email'] = email_element.text

                # Extract bio
                bio_element = author_element.find("dhq:bio/tei:p", namespaces=namespaces)
                if bio_element is not None:
                    author_data['bio'] = ''.join(bio_element.itertext()).strip()
                
                authors_data.append(author_data)

            base_data['authors'] = authors_data

            # Extracting paragraphs from the body
            # Check if paragraphs are inside a <div> tag
            # Get the <body> element
            body_element = root.find(".//tei:text/tei:body", namespaces=namespaces)

            # Extract all text from the <body> element and its descendants
            body_text = ''.join(body_element.itertext()).strip()

            base_data['body_text'] = body_text

            # Then, instead of creating a separate dataframe for paragraphs, you can directly append the base_data dictionary to the all_data list:
            data_df = pd.DataFrame([base_data])

            all_data.append(data_df)

        # Convert the data list to a DataFrame
        final_df = pd.concat(all_data)
        final_df.to_csv(output_path, index=False)
    return final_df

def generate_xml_files(directory_path):
    exclude = ['old', 'converted', 'dhq', 'sample', 'recovered', 'test', 'walsh']

    # List only XML files
    xml_files = [os.path.join(dp, f) for dp, _, filenames in os.walk(directory_path) for f in filenames 
                if f.endswith('.xml') 
                and not any(ex_str in f for ex_str in exclude) 
                and not f.startswith('999') and not f.startswith('000000') and not '_' in f]
    return xml_files

def get_scraped_dhq_files():
    updated_df_output_path = "../data/missing_dhq_data.csv"
    if os.path.exists(updated_df_output_path):
        updated_df = pd.read_csv(updated_df_output_path)
    else:
        updated_df = pd.DataFrame()

    article_links_df_output_path = "../data/dhq_article_links.csv"

    if os.path.exists(article_links_df_output_path):
        article_links_df = pd.read_csv(article_links_df_output_path)
        pattern = r'.*vol/([^/]*)/([^/]*)/(\d+)/\3\.html'
        article_links_df[['volume', 'issue', 'DHQarticle-id']] = article_links_df.article_link.str.extract(pattern)
        article_links_df.volume = article_links_df.volume.apply(lambda x: x.zfill(3))
    else:
        article_links_df = pd.DataFrame()
    return article_links_df, updated_df