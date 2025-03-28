from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
import asyncio
from typing import Dict, List, Any

class HTMLParser:
    """A utility class to parse HTML and extract structured elements, with translations."""

    def __init__(self, html_content: str):
        self.soup = BeautifulSoup(html_content, "html.parser")
        self.translator = GoogleTranslator(source="auto", target="en")

    async def _translate(self, text: str) -> str:
        """Translate text to English (returns original if translation fails)."""
        if not text:
            return ""
        try:
            return await asyncio.to_thread(self.translator.translate, text)
        except Exception:
            return text  # Fallback to original text

    async def _translate_texts(self, texts: List[str]) -> List[str]:
        """Translate a list of texts asynchronously, awaiting each individually."""
        results = []
        for text in texts:
            results.append(await self._translate(text))
        return results

    async def get_title(self) -> Dict[str, str]:
        """Extract and translate the page title."""
        title = self.soup.title.get_text(strip=True) if self.soup.title else "No Title"
        return {"title": await self._translate(title)}

    async def get_headings(self) -> Dict[str, List[str]]:
        """Extract headings (h1-h5) and translate them correctly."""
        headings_map = {f"h{i}": [] for i in range(1, 6)}
        heading_texts = []
        heading_positions = []
        for level in headings_map:
            elements = self.soup.find_all(level)
            for idx, tag in enumerate(elements):
                text = tag.get_text(strip=True)
                heading_texts.append(text)
                heading_positions.append((level, idx))
        # Translate all headings in order
        translated_texts = await self._translate_texts(heading_texts)
        # Assign translations back to headings correctly
        translated_map = {f"h{i}": [] for i in range(1, 6)}
        for (level, idx), translated_text in zip(heading_positions, translated_texts):
            translated_map[level].append(translated_text)
        return translated_map

    async def get_links(self) -> List[Dict[str, str]]:
        """Extract anchor links and translate their text."""
        links = [{"text": tag.get_text(strip=True), "href": tag.get("href", "#")} for tag in self.soup.find_all("a")]
        translated_texts = await self._translate_texts(link["text"] for link in links)
        for i, text in enumerate(translated_texts):
            links[i]["text"] = text  # Ensure correct mapping
        return links

    async def get_meta_tags(self) -> Dict[str, str]:
        """Extract and translate meta tags (description & keywords)."""
        meta_tags = {meta.get("name", "").lower(): meta.get("content", "") for meta in self.soup.find_all("meta") if meta.get("name")}
        translated_texts = await self._translate_texts(meta_tags.values())
        # Correct mapping for meta tags
        translated_meta_tags = {key: translated_texts[i] for i, key in enumerate(meta_tags)}
        return translated_meta_tags

    async def get_clean_text(self) -> Dict[str, str]:
        """Extract visible text and translate it."""
        for script in self.soup(["script", "style"]):  # Remove scripts & styles
            script.extract()
        clean_text = " ".join(self.soup.get_text(separator=" ").split())
        return {"text": await self._translate(clean_text)}

    def get_scripts(self, limit: int = 3) -> Dict[str, List[str]]:
        """Extract embedded and external JavaScript sources."""
        scripts_map = {
            "embedded_scripts": [script.get_text(strip=True) for script in self.soup.find_all("script") if script.string][:limit],
            "external_scripts": [script["src"] for script in self.soup.find_all("script", src=True) if "src" in script.attrs][:limit]
        }
        return scripts_map

    async def get_images(self) -> List[Dict[str, str]]:
            """Extract image sources and translate alt text."""
            images = [{"src": tag.get("src", ""), "alt": tag.get("alt", "")} for tag in self.soup.find_all("img")]
            translated_alts = await self._translate_texts([img["alt"] for img in images])
            for i, alt_text in enumerate(translated_alts):
                images[i]["alt"] = alt_text  # Assign correctly
            return images

# The test function
# async def main():
#     import json
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
#     print(json.dumps(await parser.get_title(), indent=2))
#     print("\nExtracted Headings:")
#     print(json.dumps(await parser.get_headings(), indent=2))
#     print("\nExtracted Links:")
#     print(json.dumps(await parser.get_links(), indent=2))
#     print("\nExtracted Meta Tags:")
#     print(json.dumps(await parser.get_meta_tags(), indent=2))
#     print("\nExtracted Clean Text:")
#     print(json.dumps(await parser.get_clean_text(), indent=2))
#     print("\nExtracted Scripts:")
#     print(json.dumps(parser.get_scripts(), indent=2))
#     print("\nExtracted Images:")
#     print(json.dumps(await parser.get_images(), indent=2))

# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except RuntimeError:
#         loop = asyncio.new_event_loop()
#         asyncio.set_event_loop(loop)
#         loop.run_until_complete(main())