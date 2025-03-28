import json
import boto3
from enum import Enum
from warcio.archiveiterator import ArchiveIterator
from .batch_processor import BatchProcessor
from .data_schemas.common_crawl_processed_schema import WebpageData
from .html_parser import HTMLParser


class RawFileType(Enum):
    WAT_FILE = 1
    WARC_FILE = 2


class CommonCrawlProcessor:
    """Processes Common Crawl WARC and WAT data from AWS S3."""

    def __init__(
        self, aws_user_access_key, aws_user_secret_key, crawl_id="CC-MAIN-2025-05"
    ):
        """Initializes the AWS S3 client and sets up batch processing."""
        if not aws_user_access_key or not aws_user_secret_key:
            raise ValueError(
                "AWS credentials missing! Provide access key & secret key."
            )
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_user_access_key,
            aws_secret_access_key=aws_user_secret_key,
        )
        self.bucket_name = "commoncrawl"
        self.crawl_id = crawl_id
        self.batch_processor = BatchProcessor()
        self.raw_file_type = None
        self.file_processing_limit = 10000  # Limit the number of WARC/WAT files processed to prevent database storage overflow
        self.gz_records_limit = 10  # Limit the number of records processed per gzipped file to improve processing diversity

    def list_warc_files(self):
        """Lists up to 10000 WARC file names from AWS S3 using pagination."""
        prefix = f"crawl-data/{self.crawl_id}/segments/"  # Search in segments
        warc_files = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            for page in pages:
                if "Contents" in page:
                    # Filter only .warc.gz files and add to list
                    warc_files.extend(
                        obj["Key"]
                        for obj in page["Contents"]
                        if obj["Key"].endswith(".warc.gz")
                    )
                if len(warc_files) >= self.file_processing_limit:
                    break
            warc_files = warc_files[: self.file_processing_limit]
            print(f"First 3: {warc_files[:3]} ...")
            return warc_files
        except Exception as e:
            print(f"AWS Error fetching WARC files: {e}")
            return []

    def list_wat_files(self):
        """Lists up to 10000 WAT file names from AWS S3 using pagination."""
        prefix = f"crawl-data/{self.crawl_id}/segments/"
        wat_files = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        if "wat/" in obj["Key"] and obj["Key"].endswith(".wat.gz"):
                            wat_files.append(obj["Key"])
                            if len(wat_files) >= self.file_processing_limit:
                                print(f"First 3: {wat_files[:3]} ...")
                                return wat_files
        except Exception as e:
            print(f"Error fetching WAT file list: {e}")
        return wat_files

    def extract_warc_data(self, warc_file, page_data):
        """Extracts webpage content, HTML, and scripts from a WARC file.
        Returns empty data if html content, embeddedScripts or externalScripts are not found.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=warc_file)
            stream = response["Body"]
        except Exception as e:
            print(f"Error fetching WARC file {warc_file}: {e}")
            return {}  # Return empty page_data on error

        processed_count = 0  # Initialize a counter for processed records
        for record in ArchiveIterator(stream):
            if processed_count >= self.gz_records_limit:
                break
            if record.rec_type == "response":
                url = record.rec_headers.get_header("WARC-Target-URI")
                content_type = (
                    record.http_headers.get_header("Content-Type")
                    if record.http_headers
                    else None
                )
                if url and content_type and "text/html" in content_type:
                    try:
                        html_content = (
                            record.content_stream()
                            .read()
                            .decode("utf-8", errors="ignore")
                        )
                        parser = HTMLParser(html_content)
                        parsed_data = parser.get_scripts()
                        # Extract required fields
                        embedded_scripts = parsed_data.get("embedded_scripts", [])
                        external_scripts = parsed_data.get("external_scripts", [])

                        # Check if all required fields have data
                        if (
                            not html_content
                            or not embedded_scripts
                            or not external_scripts
                        ):
                            continue  # Skip this record

                        page_data[url] = {
                            "url": url,
                            "html": html_content,
                            "embeddedScripts": embedded_scripts,
                            "externalScripts": external_scripts,
                        }
                        processed_count += 1  # Count successfully processed records.
                    except (
                        UnicodeDecodeError,
                        AttributeError,
                        TypeError,
                        Exception,
                    ) as e:
                        print(f"Error processing WARC record for {url}: {e}")
                        continue  # Skip to the next record on error
        return page_data

    def extract_wat_data(self, wat_file, page_data):
        """Extracts metadata from a WAT file and updates or creates webpage data entries.
        Returns empty data if title or links are not found.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=wat_file)
            stream = response["Body"]
        except Exception as e:
            print(f"Error fetching WAT file {wat_file}: {e}")
            return {}  # Return empty page_data on error

        processed_count = 0  # Initialize a counter for processed records
        for record in ArchiveIterator(stream):
            if processed_count >= self.gz_records_limit:
                break
            if record.rec_type == "metadata":
                try:
                    wat_data = json.loads(
                        record.content_stream().read().decode("utf-8")
                    )
                    envelope = wat_data.get("Envelope", {})
                    header_metadata = envelope.get("WARC-Header-Metadata", {})
                    payload_metadata = envelope.get("Payload-Metadata", {})
                    http_response_metadata = payload_metadata.get(
                        "HTTP-Response-Metadata", {}
                    )
                    html_metadata = http_response_metadata.get("HTML-Metadata", {})
                    head = html_metadata.get("Head", {})

                    url = header_metadata.get("WARC-Target-URI")
                    if not url:
                        continue  # Skip if URL is missing

                    title = head.get("Title", "")
                    links = [
                        link["url"]
                        for link in html_metadata.get("Links", [])
                        if "url" in link
                    ][
                        :3
                    ]  # Limit to 3

                    # Extract HTML content
                    html_content = http_response_metadata.get("HTML", {}).get(
                        "Content", ""
                    )
                    # Check if title and links is available
                    if not title or not links:
                        continue  # Skip this record
                    page_data[url] = {
                        "url": url,
                        "html": html_content,
                        "title": title,
                        "links": links,
                    }
                    processed_count += 1  # Count successfully processed records.
                except (json.JSONDecodeError, TypeError, KeyError) as e:
                    print(f"Skipping malformed JSON in {wat_file}")
        return page_data

    def store_batch_in_mongodb(self, page_data):
        """
        Stores extracted webpage data in MongoDB using structured batch format.

        This function processes webpage data in batches and inserts or updates the database
        depending on the raw file type (WAT or WARC). It ensures that batch processing is efficient
        and that only valid data is stored.

        Args:
            page_data (dict): A dictionary where keys are URLs and values are webpage data.
        """
        # Ensure BatchProcessor is initialized before proceeding
        if not self.batch_processor:
            print("Error: BatchProcessor is not initialized.")
            return
        # Ensure there's data to store
        if not page_data:
            print("No valid webpage data to store in MongoDB.")
            return
        batch_contents = {}  # Temporary storage for batch processing
        for url, data in page_data.items():
            # Convert data dictionary to WebpageData instance
            batch_contents[url] = WebpageData.to_webpage_data(data)
            # Check if batch size limit is reached
            if len(batch_contents) >= self.batch_processor.batch_size:
                # Store batch based on file type (WAT files are inserted, WARC files are updated)
                if self.raw_file_type == RawFileType.WAT_FILE:
                    self.batch_processor.insert_webpage_data(batch_contents)
                elif self.raw_file_type == RawFileType.WARC_FILE:
                    self.batch_processor.update_webpage_data(batch_contents)
                # Clear batch_contents after storing to prepare for the next batch
                batch_contents.clear()
        # Store any remaining data that didn't reach batch size limit
        if batch_contents:
            self.batch_processor.insert_webpage_data(batch_contents)

    def process_webpage_data(self, raw_file):
        """Processes WARC and WAT files together, extracting HTML, scripts, metadata."""
        print(f"Processing: {raw_file} of type {self.raw_file_type}")
        try:
            if self.raw_file_type == RawFileType.WAT_FILE:
                page_data = self.extract_wat_data(raw_file, {})
            elif self.raw_file_type == RawFileType.WARC_FILE:
                page_data = self.extract_warc_data(raw_file, {})
            else:
                print(f"Error: Unknown file type {self.raw_file_type}")
                return
        except Exception as e:
            print(f"Error processing {raw_file}: {e}")  # Catches extraction errors
            return
        if not page_data:  # Only checks if the dictionary is empty
            print("No complete data found")
            return
        self.store_batch_in_mongodb(page_data)
        print("Finished processing...")

    def process_wat_files_in_range(self, start_idx=0, end_idx=5):
        """Processes a range of WAT file in pairs."""
        wat_files = self.list_wat_files()[start_idx:end_idx]
        self.raw_file_type = RawFileType.WAT_FILE
        for wat_file in wat_files:
            self.process_webpage_data(wat_file)
        print(f"Processed WAT files {start_idx} to {end_idx}")

    def process_warc_files_in_range(self, start_idx=0, end_idx=5):
        """Processes a range of WARC file in pairs."""
        warc_files = self.list_warc_files()[start_idx:end_idx]
        self.raw_file_type = RawFileType.WARC_FILE
        for warc_file in warc_files:
            self.process_webpage_data(warc_file)
        print(f"Processed WARC files {start_idx} to {end_idx}")


# if __name__ == "__main__":
#     import os

#     processor = CommonCrawlProcessor(
#         os.getenv("AWS_ACCESS_KEY"), os.getenv("AWS_SECRET_KEY")
#     )
#     processor.process_wat_files_in_range(0, 500)
#     print(
#         f"Total webpage batches stored in MongoDB: {processor.batch_processor.count_documents()}"
#     )
#     processor.process_warc_files_in_range(0, 650)
#     print(
#         f"Total webpage batches stored in MongoDB: {processor.batch_processor.count_documents()}"
#     )