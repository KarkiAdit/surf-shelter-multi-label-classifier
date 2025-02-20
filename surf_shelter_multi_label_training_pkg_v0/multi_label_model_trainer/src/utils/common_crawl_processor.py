import json
import boto3
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
from .batch_processor import BatchProcessor


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

    def list_warc_files(self, limit=1000):
        """Lists up to 1000 WARC file names from AWS S3 using pagination."""
        prefix = f"crawl-data/{self.crawl_id}/segments/"  # Search in segments
        warc_files = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            for page in pages:
                if "Contents" in page:
                    # Filter only .warc.gz files and add to list
                    warc_files.extend(
                        obj["Key"] for obj in page["Contents"] if obj["Key"].endswith(".warc.gz")
                    )
                if len(warc_files) >= limit:
                    break  
            warc_files = warc_files[:limit]
            print(f"First 3: {warc_files[:3]} ...")  
            return warc_files
        except Exception as e:
            print(f"AWS Error fetching WARC files: {e}")
            return []

    def list_wat_files(self, limit=1000):
        """Lists up to `limit` WAT file names from AWS S3 using the correct segment path."""
        prefix = f"crawl-data/{self.crawl_id}/segments/"
        wat_files = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        if "wat/" in obj["Key"] and obj["Key"].endswith(".wat.gz"):
                            wat_files.append(obj["Key"])
                            if len(wat_files) >= limit:
                                print(f"First 3: {wat_files[:3]} ...")  
                                return wat_files
        except Exception as e:
            print(f"Error fetching WAT file list: {e}")
        return wat_files

    def extract_warc_data(self, warc_file, page_data):
        """Extracts webpage content, HTML, and scripts from a WARC file."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=warc_file)
            stream = response["Body"]
        except Exception as e:
            print(f"Error fetching WARC file {warc_file}: {e}")

        for record in ArchiveIterator(stream):
            if record.rec_type == "response":
                url = record.rec_headers.get_header("WARC-Target-URI")
                content_type = (
                    record.http_headers.get_header("Content-Type")
                    if record.http_headers
                    else None
                )
                if url and content_type and "text/html" in content_type:
                    html_content = (
                        record.content_stream().read().decode("utf-8", errors="ignore")
                    )
                    soup = BeautifulSoup(html_content, "html.parser")
                    embedded_scripts = [
                        script.get_text(strip=True)
                        for script in soup.find_all("script")
                        if script.string
                    ][:2] # Limits to 2
                    external_scripts = [
                        script["src"]
                        for script in soup.find_all("script", src=True)
                        if "src" in script.attrs
                    ][:2] # Limits to 2
                    page_data[url] = {
                        "url": url,
                        "html": html_content,
                        "embeddedScripts": embedded_scripts,
                        "externalScripts": external_scripts,
                        "title": "",
                        "links": [],
                        "headers": [],
                    }

    def extract_wat_data(self, wat_file, page_data):
        """Extracts metadata from a WAT file and updates or creates webpage data entries."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=wat_file)
            stream = response["Body"]
        except Exception as e:
            print(f"Error fetching WAT file {wat_file}: {e}")

        for record in ArchiveIterator(stream):
            if record.rec_type == "metadata":
                try:
                    wat_data = json.loads(
                        record.content_stream().read().decode("utf-8")
                    )
                    url = (
                        wat_data.get("Envelope", {})
                        .get("WARC-Header-Metadata", {})
                        .get("WARC-Target-URI")
                    )
                    if url:
                        title = (
                            wat_data.get("Envelope", {})
                            .get("Payload-Metadata", {})
                            .get("HTTP-Response-Metadata", {})
                            .get("HTML-Metadata", {})
                            .get("Head", {})
                            .get("Title", "")
                        )
                        links = [
                            link["url"]
                            for link in wat_data.get("Envelope", {})
                            .get("Payload-Metadata", {})
                            .get("HTTP-Response-Metadata", {})
                            .get("HTML-Metadata", {})
                            .get("Links", [])
                            if "url" in link
                        ][:2] # Limits to 2
                        headers = [
                            script["src"]
                            for script in wat_data.get("Envelope", {})
                            .get("Payload-Metadata", {})
                            .get("HTTP-Response-Metadata", {})
                            .get("HTML-Metadata", {})
                            .get("Head", {})
                            .get("Scripts", [])
                            if "src" in script
                        ][:2] # Limits to 2
                        if url in page_data:
                            page_data[url]["title"] = title
                            page_data[url]["links"] = links
                            page_data[url]["headers"] = headers
                            page_data[url]["externalScripts"].extend(headers)
                        else:
                            page_data[url] = {
                                "url": url,
                                "html": None,
                                "embeddedScripts": [],
                                "externalScripts": headers,
                                "title": title,
                                "links": links,
                                "headers": headers,
                            }
                except json.JSONDecodeError:
                    print(f"Skipping malformed JSON in {wat_file}")

    def store_batch_in_mongodb(self, page_data):
        """Stores extracted webpage data in MongoDB using structured batch format."""
        if not self.batch_processor:
            print("Error: BatchProcessor is not initialized.")
            return
        if not page_data:
            print("No valid webpage data to store in MongoDB.")
            return
        batch_contents = {}
        for url, data in page_data.items():
            batch_contents[url] = data
            if len(batch_contents) >= self.batch_processor.batch_size:
                self.batch_processor.insert_webpage_data(batch_contents)
                batch_contents.clear()
        if batch_contents:
            self.batch_processor.insert_webpage_data(batch_contents)

    def process_webpage_data(self, warc_file, wat_file):
        """Processes WARC and WAT files together, extracting HTML, scripts, metadata."""
        print(f"Processing: {warc_file} & {wat_file}")
        page_data = {}
        self.extract_warc_data(warc_file, page_data)
        self.extract_wat_data(wat_file, page_data)
        self.store_batch_in_mongodb(page_data)
        print(f"Finished processing: {warc_file} & {wat_file}")

    def process_webpage_files_in_range(self, start_index=0, end_index=5):
        """Processes a range of WARC and WAT file pairs."""
        warc_files = self.list_warc_files()[start_index:end_index]
        wat_files = self.list_wat_files()[start_index:end_index]
        if not warc_files or not wat_files:
            print("No more files to process.")
            return
        for warc_file, wat_file in zip(warc_files, wat_files):
            self.process_webpage_data(warc_file, wat_file)
        print(f"Processed WARC files {start_index} to {end_index}")


# if __name__ == "__main__":
#     import os
#     processor = CommonCrawlProcessor(
#         os.getenv("AWS_ACCESS_KEY"), os.getenv("AWS_SECRET_KEY")
#     )
#     processor.process_webpage_files_in_range(0, 1)
#     print(
#         f"Total webpage batches stored in MongoDB: {processor.batch_processor.count_documents()}"
#     )