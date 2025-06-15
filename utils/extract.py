"""
Extract module for Fashion Studio ETL Pipeline
This module handles data extraction from the Fashion Studio website
"""

import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Optional
from datetime import datetime
import time


def fetch_webpage(url: str) -> Optional[str]:
    """
    Fetch HTML content from the given URL
    
    Args:
        url (str): The URL to fetch
        
    Returns:
        Optional[str]: HTML content if successful, None otherwise
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching webpage: {e}")
        return None


def parse_product_data(html_content: str, timestamp: str) -> List[Dict[str, str]]:
    """
    Parse product data from HTML content
    
    Args:
        html_content (str): HTML content to parse
        timestamp (str): Timestamp when data was extracted
        
    Returns:
        List[Dict[str, str]]: List of product dictionaries
    """
    if not html_content:
        return []
    
    soup = BeautifulSoup(html_content, 'html.parser')
    products = []
    
    # Find all product cards
    product_cards = soup.find_all('div', class_='collection-card')
    
    for card in product_cards:
        product = {}
        
        # Extract product title (name)
        title_element = card.find('h3', class_='product-title')
        product['title'] = title_element.text.strip() if title_element else 'Unknown'
        
        # Extract price
        price_element = card.find('span', class_='price')
        if price_element:
            price_text = price_element.text.strip()
            product['price'] = price_text
        else:
            product['price'] = 'Price Unavailable'
        
        # Extract rating, colors, size, and gender from paragraph elements
        detail_paragraphs = card.find_all('p', style=lambda value: value and 'font-size: 14px' in value)
        
        product['rating'] = 'Not Rated'
        product['colors'] = '0'
        product['size'] = 'Unknown'
        product['gender'] = 'Unknown'
        
        for p in detail_paragraphs:
            text = p.text.strip()
            if text.startswith('Rating:'):
                product['rating'] = text.replace('Rating: ', '').strip()
            elif 'Colors' in text:
                colors_match = re.search(r'(\d+)\s+Colors', text)
                product['colors'] = colors_match.group(1) if colors_match else '0'
            elif text.startswith('Size:'):
                product['size'] = text.replace('Size: ', '').strip()
            elif text.startswith('Gender:'):
                product['gender'] = text.replace('Gender: ', '').strip()
        
        # Add timestamp
        product['timestamp'] = timestamp
        
        products.append(product)
    
    return products


def extract_fashion_data(base_url: str = "https://fashion-studio.dicoding.dev/", 
                       max_pages: int = 50) -> List[Dict[str, str]]:
    """
    Main extraction function to get fashion product data from all pages
    
    Args:
        base_url (str): Base URL of the fashion website
        max_pages (int): Maximum number of pages to scrape (1-50)
        
    Returns:
        List[Dict[str, str]]: List of extracted product data
    """
    print(f"Extracting data from: {base_url}")
    print(f"Scraping pages 1 to {max_pages}")
    
    all_products = []
    extraction_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for page in range(1, max_pages + 1):
        # Construct URL for each page
        if page == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}?page={page}"
        
        print(f"Scraping page {page}... ", end="")
        
        # Fetch webpage content
        html_content = fetch_webpage(page_url)
        if not html_content:
            print(f"Failed to fetch page {page}")
            continue
        
        # Parse product data
        page_products = parse_product_data(html_content, extraction_timestamp)
        
        if not page_products:
            print(f"No products found on page {page}")
            # If no products found, we might have reached the end
            break
        
        all_products.extend(page_products)
        print(f"Found {len(page_products)} products")
        
        # Add small delay to be respectful to the server
        time.sleep(0.5)
    
    print(f"Successfully extracted {len(all_products)} products from {page} pages")
    return all_products


if __name__ == "__main__":
    # Test the extraction (limited to 2 pages for testing)
    data = extract_fashion_data(max_pages=2)
    for i, product in enumerate(data[:3]):  # Show first 3 products
        print(f"Product {i+1}: {product}")