import csv
import os
import logging
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
SEARCH_SERVICE_NAME = os.environ["SEARCH_SERVICE_NAME"]
SEARCH_INDEX_NAME = os.environ["SEARCH_INDEX_NAME"]
SEARCH_API_VERSION = os.environ["SEARCH_API_VERSION"]
API_KEY = os.environ["SEARCH_API_KEY"]
STORAGE_CONNECTION_STRING = os.environ["STORAGE_CONNECTION_STRING"]
BLOB_CONTAINER_NAME = os.environ["BLOB_CONTAINER_NAME"]
BLOB_FILE_NAME = os.environ["BLOB_FILE_NAME"]
CSV_ID_FIELD_NAME = os.environ["CSV_ID_FIELD_NAME"]
INDEX_ID_FIELD_NAME = os.environ["INDEX_ID_FIELD_NAME"]
SAFETY_THRESHOLD = float(os.environ["SAFETY_THRESHOLD"])
 
def get_ids_from_csv(language: str) -> set:
    # Initialize Blob Service Client

    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    container_name = BLOB_CONTAINER_NAME.format(language=language)
    blob_name = BLOB_FILE_NAME.format(language=language)
    
    container_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Read CSV file from Azure Blob Storage
    csv_ids = set()
    stream = container_client.download_blob()
    csv_reader = csv.DictReader(stream.readall().decode('utf-8').splitlines())
    for row in csv_reader:
        csv_ids.add(row[CSV_ID_FIELD_NAME])

    logging.info(f"Found {len(csv_ids)} IDs in CSV file.")

    return csv_ids

def get_ids_from_index(language: str) -> set:
    # Initialize variables for search pagination
    search_ids = set()
    headers = {"api-key": API_KEY, "Content-Type": "application/json"}
    page_size = 1000
    current_skip = 0

    # Fetch all documents from Azure Cognitive Search index with pagination
    while True:
        url = f"https://{SEARCH_SERVICE_NAME}.search.windows.net/indexes/{SEARCH_INDEX_NAME.format(language=language)}/docs/search?api-version={SEARCH_API_VERSION}"
        payload = {"select": INDEX_ID_FIELD_NAME, "top": page_size, "skip": current_skip}
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            search_results = response.json()
            for doc in search_results["value"]:
                search_ids.add(doc[INDEX_ID_FIELD_NAME])

            # Check if there are more pages
            if len(search_results["value"]) < page_size:
                break
            else:
                current_skip += page_size
        else:
            logging.error(f"Failed to fetch documents from Azure Cognitive Search: {response.json()}")
            break

    logging.info(f"Found {len(search_ids)} IDs in Azure Cognitive Search index.")
    return search_ids

def cleanup(search_ids: set, csv_ids: set, language: str) -> int:
    # Find IDs that need to be deleted
    ids_to_delete = search_ids - csv_ids

    logging.info(f"Found {len(ids_to_delete)} IDs to delete.")

    if len(ids_to_delete) > SAFETY_THRESHOLD * len(search_ids):
        logging.warning("Too many IDs to delete. Aborting.")
        return -1
 
    # Issue delete instructions to Azure Cognitive Search
    if ids_to_delete:
        actions = [{"@search.action": "delete", INDEX_ID_FIELD_NAME: id_} for id_ in ids_to_delete]
        headers = {"api-key": API_KEY, "Content-Type": "application/json"}
        url = f"https://{SEARCH_SERVICE_NAME}.search.windows.net/indexes/{SEARCH_INDEX_NAME.format(language=language)}/docs/index?api-version={SEARCH_API_VERSION}"
        response = requests.post(url, headers=headers, json={"value": actions})
        if response.status_code == 200:
            logging.info(f"Successfully deleted {len(ids_to_delete)} documents.")
            return len(ids_to_delete)
        else:
            logging.error(f"Failed to delete documents: {response.json()}")
            return -1
    else:
        logging.info("No documents to delete.")
        return 0

def execute(language) -> int:
    csv_ids = get_ids_from_csv(language)
    search_ids = get_ids_from_index(language)
    
    return cleanup(search_ids, csv_ids, language)

def whatif(language):
    csv_ids = get_ids_from_csv(language)
    search_ids = get_ids_from_index(language)

    ids_to_delete = search_ids - csv_ids

    logging.info(f"Found {len(ids_to_delete)} IDs to delete.")
