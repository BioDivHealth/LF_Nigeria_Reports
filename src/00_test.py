#!/usr/bin/env python3
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Option 1: cloudscraper
import cloudscraper

def fetch_with_cloudscraper(url: str) -> str:
    """
    Fetch HTML using cloudscraper, which solves Cloudflare IUAM challenges automatically.
    Proxy settings are read from environment variables HTTP_PROXY and HTTPS_PROXY.
    """
    # Build proxies dict if environment variables are set
    proxy = os.getenv('HTTP_PROXY') or os.getenv('HTTPS_PROXY')
    proxies = {'http': proxy, 'https': proxy} if proxy else None

    scraper = cloudscraper.create_scraper()  # returns a CloudScraper instance  [oai_citation:0‡PyPI](https://pypi.org/project/cloudscraper/?utm_source=chatgpt.com)
    resp = scraper.get(url, proxies=proxies)
    resp.raise_for_status()
    return resp.text


# Option 2: stealth-requests
import stealth_requests as stealth

def fetch_with_stealth_requests(url: str) -> str:
    """
    Fetch HTML using stealth-requests, which mimics real browser TLS fingerprints via curl_cffi.
    Proxy settings are read from environment variables HTTP_PROXY and HTTPS_PROXY.
    """
    proxy = os.getenv('HTTP_PROXY') or os.getenv('HTTPS_PROXY')
    proxies = {'http': proxy, 'https': proxy} if proxy else None

    # One-off request
    resp = stealth.get(url, proxies=proxies)  # API mirrors requests.get  [oai_citation:1‡GitHub](https://github.com/jpjacobpadilla/Stealth-Requests)
    resp.raise_for_status()
    return resp.text


if __name__ == '__main__':
    BASE_URL = "https://ncdc.gov.ng"
    LIST_PAGE_URL = (
        f"{BASE_URL}"
        "/diseases/sitreps/"
        "?cat=5&name=An%20update%20of%20Lassa%20fever%20outbreak%20in%20Nigeria"
    )

    # Choose one of the two methods:
    html_content = fetch_with_cloudscraper(LIST_PAGE_URL)
    # html_content = fetch_with_stealth_requests(LIST_PAGE_URL)

    # Save to file for later parsing
    with open('lassa_sitreps.html', 'w', encoding='utf-8') as f:
        logging.info(html_content)
        f.write(html_content)

    logging.info("Page saved to lassa_sitreps.html")