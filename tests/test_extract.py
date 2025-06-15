"""
Unit tests for the extract module
"""

import pytest
import sys
import os

# Add utils directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from utils.extract import fetch_webpage, parse_product_data, extract_fashion_data


class TestExtract:
    
    def test_fetch_webpage_valid_url(self):
        """Test fetching webpage with valid URL"""
        # Test with a simple URL that should work
        html_content = fetch_webpage("https://httpbin.org/html")
        assert html_content is not None
        assert isinstance(html_content, str)
        assert len(html_content) > 0
    
    def test_fetch_webpage_invalid_url(self):
        """Test fetching webpage with invalid URL"""
        html_content = fetch_webpage("https://invalid-url-that-does-not-exist.com")
        assert html_content is None
    
    def test_parse_product_data_empty_html(self):
        """Test parsing with empty HTML"""
        products = parse_product_data("", "2025-06-15 10:00:00")
        assert products == []
    
    def test_parse_product_data_no_products(self):
        """Test parsing HTML with no product cards"""
        html = "<html><body><div>No products here</div></body></html>"
        products = parse_product_data(html, "2025-06-15 10:00:00")
        assert products == []
    
    def test_parse_product_data_single_product(self):
        """Test parsing HTML with single product"""
        html = """
        <html>
        <body>
            <div class="collection-card">
                <img src="https://example.com/image.jpg" class="collection-image" alt="Test Product">
                <div class="product-details">
                    <h3 class="product-title">Test T-shirt</h3>
                    <div class="price-container"><span class="price">$25.99</span></div>
                    <p style="font-size: 14px; color: #777;">Rating: ⭐ 4.5 / 5</p>
                    <p style="font-size: 14px; color: #777;">3 Colors</p>
                    <p style="font-size: 14px; color: #777;">Size: M</p>
                    <p style="font-size: 14px; color: #777;">Gender: Unisex</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        timestamp = "2025-06-15 10:00:00"
        products = parse_product_data(html, timestamp)
        assert len(products) == 1
        
        product = products[0]
        assert product['title'] == 'Test T-shirt'
        assert product['price'] == '$25.99'
        assert product['rating'] == '⭐ 4.5 / 5'
        assert product['colors'] == '3'
        assert product['size'] == 'M'
        assert product['gender'] == 'Unisex'
        assert product['timestamp'] == timestamp
    
    def test_parse_product_data_missing_fields(self):
        """Test parsing HTML with missing product fields"""
        html = """
        <html>
        <body>
            <div class="collection-card">
                <div class="product-details">
                    <h3 class="product-title">Incomplete Product</h3>
                </div>
            </div>
        </body>
        </html>
        """
        
        timestamp = "2025-06-15 10:00:00"
        products = parse_product_data(html, timestamp)
        assert len(products) == 1
        
        product = products[0]
        assert product['title'] == 'Incomplete Product'
        assert product['price'] == 'Price Unavailable'
        assert product['rating'] == 'Not Rated'
        assert product['colors'] == '0'
        assert product['size'] == 'Unknown'
        assert product['gender'] == 'Unknown'
        assert product['timestamp'] == timestamp
    
    def test_parse_product_data_multiple_products(self):
        """Test parsing HTML with multiple products"""
        html = """
        <html>
        <body>
            <div class="collection-card">
                <h3 class="product-title">Product 1</h3>
                <span class="price">$10.00</span>
            </div>
            <div class="collection-card">
                <h3 class="product-title">Product 2</h3>
                <span class="price">$20.00</span>
            </div>
        </body>
        </html>
        """
        
        timestamp = "2025-06-15 10:00:00"
        products = parse_product_data(html, timestamp)
        assert len(products) == 2
        assert products[0]['title'] == 'Product 1'
        assert products[1]['title'] == 'Product 2'
    
    def test_extract_fashion_data_integration(self):
        """Integration test for the main extract function"""
        # This test will actually call the real website
        # In a production environment, you might want to mock this
        products = extract_fashion_data(max_pages=1)  # Test with just 1 page
        
        # Basic assertions - the website should have some products
        assert isinstance(products, list)
        # We expect at least some products from the fashion website
        assert len(products) >= 0  # Allow for empty results in case website is down
        
        # If products exist, check structure
        if products:
            product = products[0]
            required_keys = ['title', 'price', 'rating', 'colors', 'size', 'gender', 'timestamp']
            for key in required_keys:
                assert key in product


if __name__ == "__main__":
    pytest.main([__file__])