from .data_schemas.common_crawl_processed_schema import (
    CommonCrawlProcessed, IndexTracking
)

class BatchProcessor:
    """
    Handles batch processing and insertion of structured webpage data into MongoDB.

    This class ensures:
    - Webpage data is stored efficiently in batches (max 1000 per batch).
    - `batch_id` is auto-incremented to track inserted batches.
    - Last processed file index is stored for continuous data processing.
    """

    def __init__(self):
        """
        Initializes batch processing for MongoDB.

        - Fetches the last used `batch_id` to resume from the last stored batch.
        - No explicit MongoDB connection is required since models handle it internally.
        """
        self.batch_size = 1000  # Max 1000 records per batch
        self.batch_id = self.get_last_batch_id() + 1  # Auto-increment batch ID

    def _insert_batch(self, data):
        """
        Inserts webpage data into MongoDB using structured batch processing.

        Ensures that each batch has at most `batch_size` entries.
        If more than `batch_size` entries exist, they are stored in multiple batches.

        Args:
            data (dict): Dictionary where keys are URLs and values are structured webpage data.
        """
        batch_contents = {}  # Temporary batch container
        for url, item in data.items():
            batch_contents[url] = item  # Store as { url: webpage_data }
            if len(batch_contents) >= self.batch_size:
                CommonCrawlProcessed(batch_id=self.batch_id, contents=batch_contents).save()
                print(f"Inserted batch {self.batch_id} with {len(batch_contents)} webpages.")
                batch_contents = {}  # Reset batch contents
                self.batch_id += 1  # Increment batch ID after each full batch
        # Insert remaining data (if any)
        if batch_contents:
            CommonCrawlProcessed(batch_id=self.batch_id, contents=batch_contents).save()
            print(f"Inserted batch {self.batch_id} with {len(batch_contents)} webpages.")
            self.batch_id += 1  # Increment batch ID

    def insert_webpage_data(self, batch_contents):
        """
        Inserts structured webpage data into MongoDB in batches.

        Args:
            batch_contents (dict): Dictionary where each key is a URL and value is webpage data.
        """
        if not batch_contents:
            print("No webpage data to insert.")
            return
        self._insert_batch(batch_contents)

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

    def update_last_index(self, last_index):
        """
        Updates the last processed file index in MongoDB.

        Args:
            last_index (int): The last processed file index.
        """
        index_doc = IndexTracking.objects(_id="last_processed_index").first()
        if not index_doc:
            index_doc = IndexTracking(_id="last_processed_index")  # Create if not found
        index_doc.last_index = last_index
        index_doc.save()

    def get_last_index(self):
        """
        Retrieves the last processed file index from MongoDB.

        Returns:
            int: The last processed file index (defaults to 0 if not found).
        """
        index_doc = IndexTracking.objects(_id="last_processed_index").first()
        return index_doc.last_index if index_doc else 0  # Handle None case

# # TEST FUNCTION: Generate & Insert 30 Test Webpages
# if __name__ == "__main__":
#     from .data_schemas.common_crawl_processed_schema import WebpageData 
#     batch_processor = BatchProcessor()
#     test_batch_contents = {}
#     for i in range(1, 31):  # Generate 30 test webpages
#         url = f"https://example{i}.com"
#         webpage_data = WebpageData(
#             url=url,
#             html=f"<html><head><title>Example {i}</title></head><body>Test content {i}</body></html>",
#             embeddedScripts=[f"console.log('Test Script {i}');"],
#             externalScripts=[f"https://example{i}.com/script.js"],
#             title=f"Example {i}",
#             links=[f"https://example{i}.com/about", f"https://example{i}.com/contact"],
#             headers=[f"https://example{i}.com/analytics.js"]
#         )
#         test_batch_contents[url] = webpage_data  # Add webpage to batch
#     # Insert the generated test contents
#     batch_processor.insert_webpage_data(test_batch_contents)
#     # Print batch summary
#     print("30 Test Webpages Inserted Successfully!")
#     print(f"Total Batches in 'webpages': {batch_processor.count_documents()}")
