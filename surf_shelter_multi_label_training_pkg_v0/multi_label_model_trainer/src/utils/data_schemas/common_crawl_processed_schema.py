import mongoengine as meObj
import os
from pymongo import UpdateOne
from typing import List, Tuple, Dict
from datetime import datetime, timezone

# Connect to MongoDB
meObj.connect(db=os.getenv("COLLECTION_ID"), host=os.getenv("MONGO_URL"))


# Define the Webpage Data Schema
class WebpageData(meObj.EmbeddedDocument):
    """Represents a single webpage's extracted data."""

    url = meObj.StringField(required=True)  # Unique within its batch
    html = meObj.StringField()  # Full HTML content of the webpage
    embeddedScripts = meObj.ListField(meObj.StringField())  # Inline JavaScript
    externalScripts = meObj.ListField(
        meObj.StringField()
    )  # External JavaScript sources (URLs)
    title = meObj.StringField()  # Title of the webpage (from <title> tag)
    links = meObj.ListField(
        meObj.StringField()
    )  # Outbound links present in the webpage
    headers = meObj.ListField(meObj.StringField())  # Script references (headers)

    def to_dict(self) -> dict:
        """
        Converts the WebpageData object to a dictionary, removing fields that are None.

        Returns:
            dict: A dictionary representation of the webpage data without None values.
        """
        return {
            key: value
            for key, value in {
                "url": self.url,
                "html": self.html,
                "embeddedScripts": self.embeddedScripts,
                "externalScripts": self.externalScripts,
                "title": self.title,
                "links": self.links,
                "headers": self.headers,
            }.items()
            if value is not None
        }

    def to_webpage_data(data):
        """
        Converts a dictionary to a WebpageData instance.

        Args:
            data (dict): Dictionary containing webpage details.

        Returns:
            WebpageData: An instance of WebpageData populated with the given data.
        """
        return WebpageData(**data)


# Define the Common Crawl Processed Schema
class CommonCrawlProcessed(meObj.Document):
    """Schema for storing processed Common Crawl data in MongoDB."""

    batch_id = meObj.IntField(required=True, unique=True)  # Unique batch identifier
    contents = meObj.MapField(
        meObj.EmbeddedDocumentField(WebpageData)
    )  # Store webpages as a Dict (URL -> WebpageData)
    meta = {"collection": "webpages"}

    @classmethod
    def update_batch(cls, batch_id, webpages_update_map):
        """
        Inserts or updates multiple webpage records in a given batch, preserving existing fields.

        This method:
        - Creates a new batch if it does not exist.
        - Updates existing webpage data while retaining unchanged fields.
        - Adds new webpages if they are not already present in the batch.
        - Ensures data consistency by using MongoDB atomic operations.

        Args:
            batch_id (int): Unique identifier for the batch in which webpages are stored.
            webpages_update_map (dict): A mapping of encoded URLs (keys) to `WebpageData` objects (values).
                                        Each entry represents a webpage to be updated or inserted.

        Returns:
            dict: The updated batch contents with the modified or newly added webpage data.
        """
        # Fetch batch or create a new one
        batch = cls.objects(batch_id=batch_id).first()
        if not batch:
            batch = cls(batch_id=batch_id, contents={})  # Create new batch
        # Loop through all the pages in the map
        for encoded_url, page_data in webpages_update_map.items():
            try:
                # Ensure page_data is a WebpageData instance
                if not isinstance(page_data, WebpageData):
                    page_data = WebpageData.to_webpage_data(
                        page_data
                    )  # Convert dict to WebpageData instance
                if encoded_url in batch.contents:
                    # Merge existing fields instead of overwriting
                    existing_data = batch.contents[encoded_url].to_mongo()
                    new_data = page_data.to_mongo()
                    # Update only the non-null values
                    for key, value in new_data.items():
                        if isinstance(value, (list, dict, set)) and not value:
                            continue  # Skip empty object updates
                        if (
                            value is not None and key != "url"
                        ):  # Keep URL unchanged and ignore null values
                            existing_data[key] = value
                    batch.contents[encoded_url] = WebpageData.to_webpage_data(
                        existing_data
                    )  # Convert back to instance
                else:
                    batch.contents[encoded_url] = (
                        page_data  # Store as WebpageData instance
                    )
            except (TypeError, ValueError) as e:
                print(f"Error processing {encoded_url}: {e}")
        # Save batch with updated contents
        batch.save()
        return batch.contents


# Define the Index Tracking Schema
class IndexTracking(meObj.Document):
    """Tracks the last processed batch index and item in MongoDB."""

    _id = meObj.StringField(primary_key=True, default="last_processed_index")
    last_batch_id = meObj.IntField(required=True, default=0)  # Last processed batch ID
    last_item_index = meObj.IntField(
        required=True, default=0
    )  # Last processed item index in that batch
    updated_at = meObj.DateTimeField(
        default=lambda: datetime.now(timezone.utc)
    )  # Timezone-aware timestamp

    meta = {"collection": "index_tracking"}


# Tracks unique webpage URLs and their lookup data.
class WebpageUrlLookup(meObj.Document):
    pageUrl = meObj.StringField(required=True, unique=True)  # URL Key (Encoded)
    batch_id = meObj.IntField(
        required=True
    )  # The ID of the batch associated with the URL lookup.
    meta = {"collection": "url_lookup_table"}

    @classmethod
    def bulk_update_webpage_lookup(cls, updates: List[Tuple[str, int]]):
        """
        Performs a batch update on the `WebpageUrlLookup` collection.
        """
        if not updates:
            return  # No updates to process
        bulk_operations = []
        collection = cls._get_collection()  # Get the raw MongoDB collection
        for safe_url_key, batch_id in updates:
            bulk_operations.append(
                UpdateOne(
                    {"pageUrl": safe_url_key},
                    {"$set": {"batch_id": batch_id}},  # Update batch_id for the URL
                    upsert=True,  # Insert if it doesn't exist
                )
            )
        # Execute bulk update operation
        if bulk_operations:
            collection.bulk_write(bulk_operations)

    @classmethod
    def bulk_data_lookup(cls, encoded_page_urls: List[str]) -> Dict[str, int]:
        """
        Fetches batch IDs for a list of encoded webpage URLs.

        Args:
            page_urls (List[str]): List of encoded webpage URLs.

        Returns:
            Dict[str, int]: A mapping of pageUrl -> batch_id for found documents.
        """
        if not encoded_page_urls:
            return {}  # Return empty dict if input is empty
        # Fetch all matching documents from MongoDB
        lookups = cls.objects(pageUrl__in=encoded_page_urls)
        # Map pageUrl -> batch_id
        return {doc.pageUrl: doc.batch_id for doc in lookups}


# # Test function
# if __name__ == "__main__":
#     print("MongoDB Connection Established!")
#     # Test database connection
#     db_name = os.getenv("COLLECTION_ID")
#     if not db_name:
#         print("COLLECTION_ID environment variable is not set.")
#     else:
#         print(f"Using Database: {db_name}")
#     # Sample batch ID
#     test_batch_id = 1
#     # Example encoded URLs and corresponding webpage data
#     test_webpages_update_map = {
#         "example_com": WebpageData(
#             url="https://example.com",
#             html="<html><head><title>Example Page</title></head><body>Test</body></html>",
#             embeddedScripts=["console.log('Hello World');"],
#             externalScripts=["https://example.com/script.js"],
#             title="Example Page",
#             links=["https://example.com/about"],
#             headers=["https://example.com/analytics.js"]
#         ),
#         "another_example_com": WebpageData(
#             url="https://another-example.com",
#             html="<html><head><title>Another Example</title></head><body>Test</body></html>",
#             embeddedScripts=["console.log('Test Another');"],
#             externalScripts=["https://another-example.com/script.js"],
#             title="Another Example Page",
#             links=["https://another-example.com/contact"],
#             headers=["https://another-example.com/tracker.js"]
#         )
#     }
#     # Insert or update batch
#     updated_batch_contents = CommonCrawlProcessed.update_batch(test_batch_id, test_webpages_update_map)
#     # Print the updated batch
#     print(f"\nUpdated Batch {test_batch_id}:")
#     for encoded_url, webpage_data in updated_batch_contents.items():
#         print(f"{encoded_url} → Title: {webpage_data.title}, HTML Length: {len(webpage_data.html)} chars")

#     # Verify the batch exists in MongoDB
#     found_batch = CommonCrawlProcessed.objects(batch_id=test_batch_id).first()
#     if found_batch:
#         print(f"Found Batch {test_batch_id} in MongoDB!")
#     else:
#         print(f"Batch {test_batch_id} not found in MongoDB!")
#     # Prepare updates for the lookup table
#     lookup_updates = [(encoded_url, test_batch_id) for encoded_url in test_webpages_update_map.keys()]
#     # Perform bulk update on the lookup table
#     WebpageUrlLookup.bulk_update_webpage_lookup(lookup_updates)
#     # Verify lookup table updates
#     print("\nChecking Lookup Table Entries:")
#     for encoded_url, _ in lookup_updates:
#         lookup_entry = WebpageUrlLookup.objects(pageUrl=encoded_url).first()
#         if lookup_entry:
#             print(f"Lookup Updated: {encoded_url} → Batch ID: {lookup_entry.batch_id}")
#         else:
#             print(f"Lookup Entry Missing: {encoded_url}")
