import logging
from typing import Dict, List, Optional
from .data_schemas.common_crawl_processed_schema import CommonCrawlProcessed

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
