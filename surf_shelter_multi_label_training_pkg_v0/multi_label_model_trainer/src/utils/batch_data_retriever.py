import logging
from typing import Dict, List, Optional
from .data_schemas.common_crawl_processed_schema import CommonCrawlProcessed
from .html_parser import HTMLParser

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BatchDataRetriever:
    """Handles retrieval of batch data based on batch IDs."""

    def __init__(self, batch_ids: List[int]):
        """
        Initializes the BatchDataRetriever.

        Args:
            batch_ids (List[int]): A list of unique batch identifiers.
        """
        self.batch_ids = batch_ids

    def get_batches_data(self) -> Dict[int, Optional[Dict]]:
        """
        Retrieves the contents of multiple batches from MongoDB.

        Returns:
            Dict[int, Optional[Dict]]: A dictionary mapping batch IDs to their contents.
                                       If a batch is not found, it maps to None.
        """
        try:
            # Fetch all matching batches in a single query
            batches = CommonCrawlProcessed.objects(batch_id__in=self.batch_ids)
            # Convert to dictionary {batch_id -> contents}
            batch_data_map = {batch.batch_id: batch.contents for batch in batches}
            # Ensure all requested batch_ids exist in output (None for missing ones)
            result = {
                batch_id: batch_data_map.get(batch_id, None)
                for batch_id in self.batch_ids
            }
            logger.info(
                f"Retrieved {len(batches)} batches out of {len(self.batch_ids)} requested."
            )
            return result
        except Exception as e:
            logger.error(f"Error retrieving batches: {e}")
            return {}

def fetch_content():
    """Fetch processed Common Crawl content from batches 105 and 114, extract titles and headings from HTML."""
    batch_ids = list(range(104, 115))  # Batches 105 and 114
    retriever = BatchDataRetriever(batch_ids)
    batch_data = retriever.get_batches_data()
    url_content_pairs = []
    for batch in batch_data.values():
        if batch:
            for webpage in batch.values():
                parser = HTMLParser(webpage.html)  # Initialize parser
                content_pair = extract_content(parser, webpage.url)
                if content_pair:  # Only append if content_pair is not None
                    url_content_pairs.append(content_pair)
    return url_content_pairs

def extract_content(parser, url):
    """Extracts title and headings asynchronously."""
    title_dict = parser.get_title()
    headings_dict = parser.get_headings()
    title = title_dict.get("title", "No Title")
    headings = sum(headings_dict.values(), [])  # Flatten heading lists
    content_list = (
        [title] + headings if headings else [title]
    )  # Ensure it's always a list
    return url, content_list  # Always return a tuple

# # Usage Example
# if __name__ == "__main__":
#     batch_ids = [1, 50, 115]  # Example batch IDs
#     retriever = BatchDataRetriever(batch_ids)
#     batch_data_map = retriever.get_batches_data()

#     for batch_id, contents in batch_data_map.items():
#         if contents:
#             print(f"Batch {batch_id} retrieved with {len(contents)} webpages.")
#         else:
#             print(f"Batch {batch_id} not found.")
