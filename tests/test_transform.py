"""
Unit tests for the transform module
"""

import pytest
import pandas as pd
import sys
import os

# Add utils directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from utils.transform import (
    clean_price, clean_rating, clean_colors, standardize_size, 
    standardize_gender, is_valid_title, transform_fashion_data
)


class TestTransform:
    
    def test_clean_price_valid_prices(self):
        """Test price cleaning with valid price strings (converted to IDR)"""
        assert clean_price("$100.00") == 1600000.0  # 100 * 16000
        assert clean_price("$25.99") == 415840.0    # 25.99 * 16000
        assert clean_price("$1,234.56") == 19752960.0  # 1234.56 * 16000
        assert clean_price("123.45") == 1975200.0   # 123.45 * 16000
    
    def test_clean_price_invalid_prices(self):
        """Test price cleaning with invalid price strings"""
        assert clean_price("Price Unavailable") == 0.0
        assert clean_price("") == 0.0
        assert clean_price(None) == 0.0
        assert clean_price("Not a price") == 0.0
    
    def test_clean_rating_valid_ratings(self):
        """Test rating cleaning with valid rating strings"""
        assert clean_rating("⭐ 4.5 / 5") == 4.5
        assert clean_rating("⭐ 3.2 / 5") == 3.2
        assert clean_rating("⭐ 2.8 / 5") == 2.8
    
    def test_clean_rating_invalid_ratings(self):
        """Test rating cleaning with invalid rating strings"""
        assert clean_rating("Not Rated") is None
        assert clean_rating("⭐ Invalid Rating / 5") is None
        assert clean_rating("") is None
        assert clean_rating(None) is None
    
    def test_clean_colors(self):
        """Test colors cleaning"""
        assert clean_colors("3") == 3
        assert clean_colors("5") == 5
        assert clean_colors("0") == 0
        assert clean_colors("") is None
        assert clean_colors("invalid") is None
        assert clean_colors(None) is None
    
    def test_standardize_size(self):
        """Test size standardization"""
        assert standardize_size("M") == "M"
        assert standardize_size("m") == "M"
        assert standardize_size("XL") == "XL"
        assert standardize_size("xl") == "XL"
        assert standardize_size("Unknown") is None
        assert standardize_size("") is None
        assert standardize_size("InvalidSize") is None
        assert standardize_size(None) is None
    
    def test_standardize_gender(self):
        """Test gender standardization"""
        assert standardize_gender("Men") == "Men"
        assert standardize_gender("men") == "Men"
        assert standardize_gender("WOMEN") == "Women"
        assert standardize_gender("unisex") == "Unisex"
        assert standardize_gender("Unknown") is None
        assert standardize_gender("") is None
        assert standardize_gender(None) is None
        assert standardize_gender("InvalidGender") is None
    
    def test_is_valid_title(self):
        """Test title validation"""
        assert is_valid_title("T-shirt 1") == True
        assert is_valid_title("Valid Product") == True
        assert is_valid_title("Unknown Product") == False
        assert is_valid_title("unknown") == False
        assert is_valid_title("") == False
        assert is_valid_title(None) == False
    
    def test_transform_fashion_data_empty_input(self):
        """Test transformation with empty input"""
        result = transform_fashion_data([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_transform_fashion_data_single_product(self):
        """Test transformation with single product"""
        raw_data = [
            {
                'title': 'T-shirt 1',
                'price': '$25.99',
                'rating': '⭐ 4.5 / 5',
                'colors': '3',
                'size': 'M',
                'gender': 'Unisex',
                'timestamp': '2025-06-15 10:00:00'
            }
        ]
        
        result = transform_fashion_data(raw_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        
        # Check transformed values
        row = result.iloc[0]
        assert row['title'] == 'T-shirt 1'
        assert row['price'] == 415840.0  # 25.99 * 16000
        assert row['rating'] == 4.5
        assert row['colors'] == 3
        assert row['size'] == 'M'
        assert row['gender'] == 'Unisex'
        assert row['timestamp'] == '2025-06-15 10:00:00'
    
    def test_transform_fashion_data_multiple_products(self):
        """Test transformation with multiple products"""
        raw_data = [
            {
                'title': 'T-shirt 1',
                'price': '$25.99',
                'rating': '⭐ 4.5 / 5',
                'colors': '3',
                'size': 'M',
                'gender': 'Men',
                'timestamp': '2025-06-15 10:00:00'
            },
            {
                'title': 'Hoodie 2',
                'price': '$150.00',
                'rating': '⭐ 3.8 / 5',
                'colors': '2',
                'size': 'L',
                'gender': 'Women',
                'timestamp': '2025-06-15 10:00:00'
            }
        ]
        
        result = transform_fashion_data(raw_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        # Check basic data
        assert result.iloc[0]['title'] == 'T-shirt 1'
        assert result.iloc[1]['title'] == 'Hoodie 2'
        assert result.iloc[0]['price'] == 415840.0  # 25.99 * 16000
        assert result.iloc[1]['price'] == 2400000.0  # 150.00 * 16000
    
    def test_transform_fashion_data_invalid_data(self):
        """Test transformation with invalid/missing data (should be filtered out)"""
        raw_data = [
            {
                'title': 'Unknown Product',
                'price': 'Price Unavailable',
                'rating': 'Not Rated',
                'colors': '',
                'size': '',
                'gender': '',
                'timestamp': '2025-06-15 10:00:00'
            }
        ]
        
        result = transform_fashion_data(raw_data)
        
        assert isinstance(result, pd.DataFrame)
        # Invalid data should be filtered out
        assert len(result) == 0
    
    def test_transform_fashion_data_duplicate_removal(self):
        """Test that duplicates are removed"""
        raw_data = [
            {
                'title': 'T-shirt 1',
                'price': '$25.99',
                'rating': '⭐ 4.5 / 5',
                'colors': '3',
                'size': 'M',
                'gender': 'Men',
                'timestamp': '2025-06-15 10:00:00'
            },
            {
                'title': 'T-shirt 1',  # Duplicate
                'price': '$25.99',
                'rating': '⭐ 4.5 / 5',
                'colors': '3',
                'size': 'M',
                'gender': 'Men',
                'timestamp': '2025-06-15 10:00:00'
            }
        ]
        
        result = transform_fashion_data(raw_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1  # Duplicate should be removed


if __name__ == "__main__":
    pytest.main([__file__])