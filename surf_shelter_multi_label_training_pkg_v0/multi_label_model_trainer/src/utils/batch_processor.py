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
        self.last_batch = None  # Initialize with no batch data processed yet
        self._initialize_last_batch_values()  # Set up initial values for batch tracking

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
        # Initialize the last processed batch info
        self._append_last_batch_info()

    def _append_last_batch_info(self):
        """
        Loads and appends data from the last processed batch, if available.
        If no previous batch exists, initializes a new batch ID.

        This function ensures that batch processing can resume from the last stored state
        or start fresh if no prior batch exists.

        Updates:
            - `self.batch_contents` (dict): Stores the contents of the last processed batch
            if available; otherwise, initializes as an empty dictionary.
            - `self.batch_id` (int): Retains the last updated batch ID if data exists;
            otherwise, increments it for a new batch.
        """
        # If previous data exists, load it; otherwise, initialize a new batch
        if self.last_item_index > 0:
            self.batch_contents = (
                self.last_batch if self.last_batch else self._fetch_last_batch()
            )
            self.batch_id = self.last_updated_batch_id  # Retain the existing batch ID
        else:
            self.batch_contents = {}  # No previous batch data, start fresh
            self.batch_id = self.last_updated_batch_id + 1  # Increment for a new batch

    def _fetch_last_batch(self):
        """
        Retrieves the contents of the last processed batch from the database.

        Returns:
            dict: The contents of the last batch if available; otherwise, an empty dictionary.
        """
        last_batch_record = CommonCrawlProcessed.objects(
            batch_id=self.last_updated_batch_id
        ).first()
        # Ensure the batch record exists before accessing its contents
        return (
            last_batch_record.contents
            if last_batch_record and hasattr(last_batch_record, "contents")
            else {}
        )

    def _insert_batch(self, data):
        """
        Inserts webpage data into MongoDB using batch processing.

        Ensures each batch has at most `batch_size` entries, storing extra data in new batches.

        Args:
            data (dict): A mapping of encoded URLs (keys) to webpage data (values).
        """
        lookup_updates = []  # Bulk updates for webpage lookup table
        for safe_url_key, item in data.items():
            # Prepare lookup update for batch processing
            lookup_updates.append((safe_url_key, self.batch_id))
            self.batch_contents[safe_url_key] = (
                item  # Store as { base64_url: webpage_data }
            )
            if len(self.batch_contents) >= self.batch_size:
                self._process_batch(lookup_updates)
        # Insert remaining data (if any)
        if self.batch_contents:
            self._process_batch(lookup_updates, True)

    def _process_batch(self, lookup_updates, isPartialBatch=False):
        """
        Saves the current batch data, updates lookup entries, and manages batch state.

        Args:
            lookup_updates (list): A list of tuples containing (encoded_url, batch_id) for lookup updates.
            is_partial_batch (bool, optional): Indicates if this is a partial batch with fewer than `batch_size` entries. Defaults to False.

        Updates:
            - Saves batch data to `CommonCrawlProcessed`.
            - Updates `WebpageUrlLookup` lookup entries.
            - Manages `last_updated_batch_id`, `last_item_index`, and `last_batch` to keep track of batch processing state.
            - Calls `_append_last_batch_info()` to update the last processed batch.
        """
        CommonCrawlProcessed.update_batch(self.batch_id, self.batch_contents)
        WebpageUrlLookup.bulk_update_webpage_lookup(lookup_updates)
        # Update last batch info
        self.last_updated_batch_id = self.batch_id
        self.last_item_index = len(self.batch_contents) if isPartialBatch else 0
        self.last_batch = self.batch_contents if isPartialBatch else {}
        pages_in_this_batch = self.last_item_index if isPartialBatch else 100
        print(f"Inserted batch {self.batch_id} with {pages_in_this_batch} webpages.")
        # Append the last processed batch info for continuity
        self._append_last_batch_info()
        # Reset lookup updates
        lookup_updates.clear()

    def _update_in_batches(self, data):
        """
        Updates webpage data in MongoDB in batch mode.

        Args:
            data (dict): A mapping of batch IDs to dictionaries where each dictionary
                        maps encoded URLs to WebpageData objects.
        """
        for batch_id, webpages_update_map in data.items():
            CommonCrawlProcessed.update_batch(batch_id, webpages_update_map)

    def insert_webpage_data(self, batch_contents: Dict[str, WebpageData]):
        """
        Inserts structured webpage data into MongoDB in batches and updates the last batch index tracking.

        Args:
            batch_contents (dict): Dictionary where each key is a URL and value is webpage data.
        """
        if not batch_contents:
            print("No webpage data to insert.")
            return
        self._insert_batch(
            self.map_encoded_urls_to_data(batch_contents)
        )  # Encode URLs as Base64 keys and convert corresponding page data into dictionary format
        self.update_index_tracking()

    def update_webpage_data(self, batch_contents: Dict[str, WebpageData]):
        """
        Updates webpage data in MongoDB by inserting new records and updating missing fields.

        - Inserts new webpages that are not yet in the database.
        - Updates existing webpages with missing fields.
        - Uses batch processing for efficient MongoDB updates.

        Args:
            batch_contents (dict): A mapping of URLs (keys) to WebpageData objects (values).
        """
        if not batch_contents:
            print("No webpage data to insert.")
            return
        new_contents = {}  # Encoded URL -> WebpageData (for new entries)
        inserted_contents = {}  # Batch ID -> {Encoded URL -> WebpageData}
        # Encode URLs as Base64 keys and convert corresponding page data into dictionary format
        safe_url_map = self.map_encoded_urls_to_data(batch_contents)
        # Fetch only the required fields and construct the lookup dictionary
        existing_lookups = WebpageUrlLookup.bulk_data_lookup(safe_url_map.keys())
        # Classify contents based on prior existence
        for encoded_url, page_data in safe_url_map.items():
            if encoded_url in existing_lookups:
                curr_batch_id = existing_lookups[encoded_url]
                if curr_batch_id not in inserted_contents:
                    inserted_contents[curr_batch_id] = {}
                inserted_contents[curr_batch_id][encoded_url] = page_data
            else:
                new_contents[encoded_url] = page_data
        # Batch process missing webpages
        if new_contents:
            self._insert_batch(new_contents)
        # Update existing webpages in batches
        if inserted_contents:
            self._update_in_batches(inserted_contents)

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

    def map_encoded_urls_to_data(self, batch_contents) -> Dict[str, WebpageData]:
        """
        Converts a batch of webpage data into a dictionary where URLs are encoded
        in Base64 as keys and their corresponding page data is stored in dictionary format.

        Args:
            batch_contents (Dict[str, WebpageData]): A dictionary mapping URLs to their respective webpage data.

        Returns:
            Dict[str, WebpageData]: A dictionary with Base64-encoded URLs as keys and page data in dictionary format.
        """
        return {
            self.get_base64_encoded(url): WebpageData.to_dict(page_data)
            for url, page_data in batch_contents.items()
        }


# Test function
if __name__ == "__main__":
    batch_processor = BatchProcessor()
    IndexTracking.objects.delete()
    WebpageUrlLookup.objects.delete()
    CommonCrawlProcessed.objects.delete()
    # Print batch processer's initialized values
    print(vars(batch_processor))
    test_batch_contents = {}
    for i in range(1, 231):  # Generate 230 test webpages
        url = f"https://example{i}.com"
        webpage_data = WebpageData(
            url=url,
            html=f"<html><head><title>Example {i}</title></head><body>Test content {i}</body></html>",
            embeddedScripts=[f"console.log('Test Script {i}');"],
            externalScripts=[f"https://example{i}.com/script.js"],
        )
        test_batch_contents[url] = webpage_data  # Add webpage to batch
    # Insert the generated test contents
    batch_processor.insert_webpage_data(test_batch_contents)
    # Print batch summary
    print("\n230 Test Webpages Inserted Successfully!")
    print(f"Total Batches in 'webpages': {batch_processor.count_documents()}")
    print(f"Next batch id: {batch_processor.batch_id}")
    # Create a mixture of previously added and new batch contents for updating
    test_update_contents = {}
    for i in range(1, 236): # Last 5 page data will be inserted. Remaining will be updated.
        url = f"https://example{i}.com"
        webpage_data = WebpageData(
            url=url,
            html="Updated",
            title=f"Example {i}",
            links=[f"https://example{i}.com/about", f"https://example{i}.com/contact"],
            headers=[f"https://example{i}.com/analytics.js"],
        )
        test_update_contents[url] = webpage_data  # Add webpage to batch
    batch_processor.update_webpage_data(test_update_contents)
    print(f"Total batches: {batch_processor.count_documents()}")
    print(f"Next batch id: {batch_processor.batch_id}")
