import base64
from .data_schemas.common_crawl_processed_schema import (
    CommonCrawlProcessed,
    IndexTracking,
    WebpageUrlLookup,
    WebpageData,
)
from typing import Dict
from datetime import datetime, timezone


class BatchProcessor:
    """
    Handles batch processing and insertion of structured webpage data into MongoDB.

    This class ensures:
    - Webpage data is stored efficiently in batches (max 1000 per batch).
    - `batch_id` is auto-incremented to track inserted batches.
    - Last processed file index is stored for continuous data processing.
    - The last stored batch ID and item index are retrieved from MongoDB.
    - The processor persists state across runs, ensuring seamless batch tracking.
    """

    def __init__(self):
        """
        Initializes batch processing with necessary configurations.
        """
        self.batch_size = 100  # Max 100 records per batch
        self._initialize_last_batch_values()

    def _initialize_last_batch_values(self):
        """
        Loads the last processed batch state from MongoDB.

        Retrieves the last batch ID, item index, and batch data, defaulting to 0 if none exist.
        """
        # Fetch the last processed index from MongoDB
        index_doc = IndexTracking.objects(_id="last_processed_index").first()
        # Set values based on existing data or defaults
        self.last_updated_batch_id = index_doc.last_batch_id if index_doc else 0
        self.last_item_index = index_doc.last_item_index if index_doc else 0
        # Fetch the last processed batch data, or set an empty dict if it doesn't exist
        self.last_batch_data = (
            CommonCrawlProcessed.objects(batch_id=self.last_updated_batch_id).first()
            or {}
        )
        # Increment last updated batch ID by 1 for the next batch
        self.batch_id = self.last_updated_batch_id + 1

    def _insert_batch(self, data):
        """
        Inserts webpage data into MongoDB using batch processing.

        Ensures each batch has at most `batch_size` entries, storing extra data in new batches.

        Args:
            data (dict): A mapping of URLs (keys) to webpage data (values).
        """
        batch_contents = self.last_batch_data  # Temporary batch container
        lookup_updates = []  # Bulk updates for webpage lookup table
        for url, item in data.items():
            # Encode the URL to remove special characters
            safe_url_key = self.get_base64_encoded(url)
            # Prepare lookup update for batch processing
            lookup_updates.append((safe_url_key, self.batch_id))
            batch_contents[safe_url_key] = item  # Store as { base64_url: webpage_data }
            if len(batch_contents) >= self.batch_size:
                self._process_batch(batch_contents, lookup_updates)
        # Insert remaining data (if any)
        if batch_contents:
            self._process_batch(batch_contents, lookup_updates, True)

    def _process_batch(self, batch_contents, lookup_updates, isPartialBatch=False):
        """
        Handles batch insertion and lookup updates.

        Args:
            batch_contents (dict): A mapping of encoded URLs to webpage data.
            lookup_updates (list): A list of tuples containing (encoded_url, batch_id) for lookup updates.
            isPartialBatch (bool, optional): Indicates if this is a final batch with fewer than `batch_size` entries. Defaults to False.
        """
        CommonCrawlProcessed(batch_id=self.batch_id, contents=batch_contents).save()
        WebpageUrlLookup.bulk_update_webpage_lookup(lookup_updates)
        # Update last batch info
        self.last_updated_batch_id = self.batch_id
        self.last_item_index = len(batch_contents) if isPartialBatch else 0
        self.last_batch_data = batch_contents
        pages_in_this_batch = self.last_item_index if isPartialBatch else 100
        print(f"Inserted batch {self.batch_id} with {pages_in_this_batch} webpages.")
        # Reset batch storage and increment batch ID
        batch_contents.clear()
        lookup_updates.clear()
        self.batch_id += 1

    def insert_webpage_data(self, batch_contents: Dict[str, WebpageData]):
        """
        Inserts structured webpage data into MongoDB in batches and updates the last batch index tracking.

        Args:
            batch_contents (dict): Dictionary where each key is a URL and value is webpage data.
        """
        if not batch_contents:
            print("No webpage data to insert.")
            return
        self._insert_batch(batch_contents)
        self.update_index_tracking()

    def get_last_batch_id(self):
        """
        Retrieves the last used `batch_id` from MongoDB.

        Returns:
            int: The last used `batch_id` (defaults to 0 if no batch exists).
        """
        last_batch = CommonCrawlProcessed.objects.order_by("-batch_id").first()
        return last_batch.batch_id if last_batch else 0  # Handle None case

    def count_documents(self):
        """
        Returns the total number of stored batches.

        Returns:
            int: The number of stored batches.
        """
        return CommonCrawlProcessed.objects.count()

    def update_index_tracking(self):
        """
        Updates the last processed batch index in MongoDB.
        """
        # Perform an atomic update or create new entry if it doesn't exist
        IndexTracking.objects(_id="last_processed_index").modify(
            upsert=True,
            set__last_batch_id=self.last_updated_batch_id,
            set__last_item_index=self.last_item_index,
            set__updated_at=datetime.now(timezone.utc),  # Auto-update timestamp
        )

    def get_base64_encoded(self, url: str):
        return base64.urlsafe_b64encode(url.encode()).decode()


# # Test function
# if __name__ == "__main__":
#     batch_processor = BatchProcessor()
#     test_batch_contents = {}
#     for i in range(1, 231):  # Generate 230 test webpages
#         url = f"https://example{i}.com"
#         webpage_data = WebpageData(
#             url=url,
#             html=f"<html><head><title>Example {i}</title></head><body>Test content {i}</body></html>",
#             embeddedScripts=[f"console.log('Test Script {i}');"],
#             externalScripts=[f"https://example{i}.com/script.js"],
#             title=f"Example {i}",
#             links=[f"https://example{i}.com/about", f"https://example{i}.com/contact"],
#             headers=[f"https://example{i}.com/analytics.js"],
#         )
#         test_batch_contents[url] = webpage_data  # Add webpage to batch
#     # Insert the generated test contents
#     batch_processor.insert_webpage_data(test_batch_contents)
#     # Print batch summary
#     print("\n230 Test Webpages Inserted Successfully!")
#     print(f"Total Batches in 'webpages': {batch_processor.count_documents()}")
#     print(f"Next batch id: {batch_processor.batch_id}")
