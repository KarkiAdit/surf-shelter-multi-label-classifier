import mongoengine as meObj
import os

# Connect to MongoDB (Ensure correct MongoDB URL)
meObj.connect(db="surf_shelter_datasets", host=os.getenv("MONGO_URL"))

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
    contents = meObj.MapField(meObj.EmbeddedDocumentField(WebpageData))  # Store webpages as a Dict (URL -> WebpageData)
    meta = {"collection": "webpages"}

    # Add a method to find or create a webpage entry
    @classmethod
    def find_or_create(cls, batch_id, url, **kwargs):
        batch = cls.objects(batch_id=batch_id).first()
        if not batch:
            batch = cls(batch_id=batch_id, contents={})  # Initialize `contents` as a Dict
        # Ensure `contents` is properly initialized
        if batch.contents is None:
            batch.contents = {}
        # Update or Create webpage data correctly
        if url in batch.contents:
            for key, value in kwargs.items():
                setattr(batch.contents[url], key, value)
        else:
            batch.contents.update({url: WebpageData(url=url, **kwargs)})
        batch.save()  # Save after modification
        return batch, batch.contents[url]

# Define the Index Tracking Schema
class IndexTracking(meObj.Document):
    """Tracks the last processed batch index in MongoDB."""
    _id = meObj.StringField(primary_key=True, default="last_processed_index")
    last_index = meObj.IntField(required=True, default=0)
    meta = {"collection": "index_tracking"}

# Represents lookup info for a batch and field completeness.
class WebpageLookupData(meObj.EmbeddedDocument):
   batch_id = meObj.IntField(required=True) # The ID of the batch associated with the URL lookup.
   has_all_fields = meObj.BinaryField(required=True) # Binary indicating if all expected fields were present.

# Tracks unique webpage URLs and their lookup data.
class WebpageUrlLookup(meObj.Document):
   pageUrl = meObj.MapField(meObj.EmbeddedDocumentField(WebpageLookupData)) # Stores lookup info as: pageURL -> LookupData
   meta = {"collection": "urlLookupTable"}

# # Test Function
# if __name__ == "__main__":
#     batch, webpage = CommonCrawlProcessed.find_or_create(
#         batch_id=1,
#         url="https://example.com",
#         html="<html><head><title>Example</title></head><body>Test</body></html>",
#         embeddedScripts=["console.log('Hello');"],
#         externalScripts=["https://example.com/script.js"],
#         title="Example",
#         links=["https://example.com/about"],
#         headers=["https://example.com/analytics.js"]
#     )
#     batch, webpage2 = CommonCrawlProcessed.find_or_create(
#         batch_id=1,
#         url="https://another.com",
#         html="<html><head><title>Another</title></head><body>Test2</body></html>",
#         embeddedScripts=["console.log('Hello2');"],
#         externalScripts=["https://another.com/script.js"],
#         title="Another",
#         links=["https://another.com/about"],
#         headers=["https://another.com/analytics.js"]
#     )
#     # Test index tracking
#     test_index = IndexTracking.objects(_id="last_processed_index").first() or IndexTracking(_id="last_processed_index")
#     test_index.last_index = 1
#     test_index.save()
#     # Check the stored data
#     print("Collections Created/Updated Successfully!")
#     print(f"Total documents in 'webpages': {CommonCrawlProcessed.objects.count()}")
#     print(f"Total documents in 'index_tracking': {IndexTracking.objects.count()}")
#     print(batch.contents)
#     print(webpage.url)
#     print(webpage2.html)
