from bs4 import BeautifulSoup
from typing import Dict, List

class HTMLParser:
    """A utility class to parse HTML and extract structured elements."""

    def __init__(self, html_content: str):
        self.soup = BeautifulSoup(html_content, "html.parser")

    def get_title(self) -> Dict[str, str]:
        """Extract the page title."""
        title = self.soup.title.get_text(strip=True) if self.soup.title else "No Title"
        return {"title": title}

    def get_headings(self) -> Dict[str, List[str]]:
        """Extract headings (h1-h5)."""
        headings_map = {f"h{i}": [] for i in range(1, 6)}
        for level in headings_map:
            elements = self.soup.find_all(level)
            for tag in elements:
                text = tag.get_text(strip=True)
                headings_map[level].append(text)
        return headings_map

    def get_links(self) -> List[Dict[str, str]]:
        """Extract anchor links."""
        links = [
            {"text": tag.get_text(strip=True), "href": tag.get("href", "#")}
            for tag in self.soup.find_all("a")
        ]
        return links

    def get_meta_tags(self) -> Dict[str, str]:
        """Extract meta tags (description & keywords)."""
        meta_tags = {
            meta.get("name", "").lower(): meta.get("content", "")
            for meta in self.soup.find_all("meta")
            if meta.get("name")
        }
        return meta_tags

    def get_clean_text(self) -> Dict[str, str]:
        """Extract visible text."""
        for script in self.soup(["script", "style"]):  # Remove scripts & styles
            script.extract()
        clean_text = " ".join(self.soup.get_text(separator=" ").split())
        return {"text": clean_text}

    def get_scripts(self, limit: int = 3) -> Dict[str, List[str]]:
        """Extract embedded and external JavaScript sources."""
        scripts_map = {
            "embedded_scripts": [
                script.get_text(strip=True)
                for script in self.soup.find_all("script")
                if script.string
            ][:limit],
            "external_scripts": [
                script["src"]
                for script in self.soup.find_all("script", src=True)
                if "src" in script.attrs
            ][:limit],
        }
        return scripts_map

    def get_images(self) -> List[Dict[str, str]]:
        """Extract image sources."""
        images = [
            {"src": tag.get("src", ""), "alt": tag.get("alt", "")}
            for tag in self.soup.find_all("img")
        ]
        return images

# if __name__ == "__main__":
#     html_content = """
#     <html>
#     <head>
#         <meta name="description" content="Esta es una página de muestra">
#         <meta name="keywords" content="clickbait, noticias, tendencias">
#         <title>惊人的新闻！你不会相信发生了什么</title>
#     </head>
#     <body>
#         <h1>主标题在这里</h1>
#         <h2>副标题震惊了你！</h2>
#         <h3>这是另一个标题</h3>

#         <a href="https://example.com/article1">Leer más sobre esto</a>
#         <a href="https://example.com/article2">Descubre más aquí</a>
#         <a href="https://example.com/article3">Haz clic para más información</a>

#         <img src="image1.jpg" alt="第一张图片">
#         <img src="image2.jpg" alt="第二张图片">
#         <img src="image3.jpg" alt="第三张图片">

#         <script>console.log('Este es un script JS 1');</script>
#         <script>console.log('Este es un script JS 2');</script>
#         <script>console.log('Este es un script JS 3');</script>

#         <script src="https://example.com/script1.js"></script>
#         <script src="https://example.com/script2.js"></script>
#         <script src="https://example.com/script3.js"></script>
#     </body>
#     </html>
#     """
#     parser = HTMLParser(html_content)
#     print("\nExtracted Title:")
#     print(parser.get_title())
#     print("\nExtracted Headings:")
#     print(parser.get_headings())
#     print("\nExtracted Links:")
#     print(parser.get_links())
#     print("\nExtracted Meta Tags:")
#     print(parser.get_meta_tags())
#     print("\nExtracted Clean Text:")
#     print(parser.get_clean_text())
#     print("\nExtracted Scripts:")
#     print(parser.get_scripts())
#     print("\nExtracted Images:")
#     print(parser.get_images())