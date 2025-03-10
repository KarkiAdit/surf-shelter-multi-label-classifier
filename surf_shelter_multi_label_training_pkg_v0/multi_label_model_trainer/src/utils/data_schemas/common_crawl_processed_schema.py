import mongoengine as meObj
import os
import base64

# Connect to MongoDB (Ensure correct MongoDB URL)
meObj.connect(db=os.getenv("COLLECTION_ID"), host=os.getenv("MONGO_URL"))

# Define the Webpage Data Schema
class WebpageData(meObj.EmbeddedDocument):
    """Represents a single webpage's extracted data."""
    url = meObj.StringField(required=True)  # Unique within its batch
    html = meObj.StringField()
    embeddedScripts = meObj.ListField(meObj.StringField())  # Inline JavaScript
    externalScripts = meObj.ListField(meObj.StringField())  # External JS sources
    title = meObj.StringField()
    links = meObj.ListField(meObj.StringField())  # Outbound links
    headers = meObj.ListField(meObj.StringField())  # Script references (headers)

# Define the Common Crawl Processed Schema (Using Dict Instead of List)
class CommonCrawlProcessed(meObj.Document):
    """Schema for storing processed Common Crawl data in MongoDB."""
    batch_id = meObj.IntField(required=True, unique=True)  # Unique batch identifier
    contents = meObj.MapField(
        meObj.EmbeddedDocumentField(WebpageData)
    )  # Store webpages as a Dict (URL -> WebpageData)
    meta = {"collection": "webpages"}

    # Returns the data of the webpage that gets added or updated
    @classmethod
    def find_or_create(cls, batch_id, url, **kwargs):
        # Encode the URL to remove special characters
        safe_url_key = base64.urlsafe_b64encode(url.encode()).decode()
        batch = cls.objects(batch_id=batch_id).first()
        if not batch:
            batch = cls(batch_id=batch_id, contents={})
        if batch.contents is None:
            batch.contents = {}
        # Retrieve or create WebpageData safely
        webpage_data = batch.contents.get(safe_url_key, WebpageData(url=url))
        # Preserve existing fields while updating new ones
        for key, value in kwargs.items():
            if key != "url":  # Prevent modifying the URL
                setattr(webpage_data, key, value)
        # Store updated webpage data back into batch.contents
        batch.contents[safe_url_key] = webpage_data
        batch.save()
        return batch.contents[safe_url_key]

# Define the Index Tracking Schema
class IndexTracking(meObj.Document):
    """Tracks the last processed batch index in MongoDB."""
    _id = meObj.StringField(primary_key=True, default="last_processed_index")
    last_index = meObj.IntField(required=True, default=0)
    meta = {"collection": "index_tracking"}

# Represents lookup info for a batch and field completeness.
class WebpageLookupData(meObj.EmbeddedDocument):
    batch_id = meObj.IntField(
        required=True
    )  # The ID of the batch associated with the URL lookup.
    has_all_fields = meObj.BinaryField(
        required=True
    )  # Binary indicating if all expected fields were present.


# Tracks unique webpage URLs and their lookup data.
class WebpageUrlLookup(meObj.Document):
    pageUrl = meObj.MapField(
        meObj.EmbeddedDocumentField(WebpageLookupData)
    )  # Stores lookup info as: pageURL -> LookupData
    meta = {"collection": "url_lookup_table"}


# Test Function
if __name__ == "__main__":
    webpage = CommonCrawlProcessed.find_or_create(
        batch_id=1,
        url="https://example5.com",
        html="<html><head><title>Example</title></head><body>Test</body></html>",
        embeddedScripts=["console.log('Hello');"],
    )
    webpage_updated = CommonCrawlProcessed.find_or_create(
        batch_id=1,
        url="https://example5.com",
        externalScripts=["https://example.com/script.js"],
        title="Example",
        links=["https://example.com/about"],
        headers=["https://example.com/analytics.js"],
    )
    # Test index tracking
    test_index = IndexTracking.objects(
        _id="last_processed_index"
    ).first() or IndexTracking(_id="last_processed_index")
    test_index.last_index = 1
    test_index.save()
    # Check the stored data
    print("Collections Created/Updated Successfully!")
    print(f"Total documents in 'webpages': {CommonCrawlProcessed.objects.count()}")
    print(f"Total documents in 'index_tracking': {IndexTracking.objects.count()}")
    print(webpage.html)
    print(webpage_updated.html)
