"""
Transform module for Fashion Studio ETL Pipeline
This module handles data transformation and cleaning
"""

import pandas as pd
import re
from typing import List, Dict, Any
from datetime import datetime

# Exchange rate USD to IDR
USD_TO_IDR_RATE = 16000


def clean_price(price_str: str) -> float:
    """
    Clean and convert price string to IDR
    
    Args:
        price_str (str): Price string (e.g., "$100.00", "Price Unavailable")
        
    Returns:
        float: Cleaned price in IDR, 0.0 if unavailable
    """
    if not price_str or price_str == "Price Unavailable":
        return 0.0
    
    # Remove currency symbols and extract numeric value
    price_match = re.search(r'[\d,]+\.?\d*', price_str.replace(',', ''))
    if price_match:
        usd_price = float(price_match.group())
        # Convert USD to IDR
        return usd_price * USD_TO_IDR_RATE
    return 0.0


def clean_rating(rating_str: str) -> float:
    """
    Clean and extract rating information as float
    
    Args:
        rating_str (str): Rating string (e.g., "⭐ 4.5 / 5", "Not Rated")
        
    Returns:
        float: Rating score as float, None if invalid
    """
    if not rating_str or rating_str == "Not Rated":
        return None
    
    # Handle "Invalid Rating" case
    if "Invalid Rating" in rating_str:
        return None
    
    # Extract rating score (e.g., "⭐ 4.5 / 5")
    rating_match = re.search(r'(\d+\.?\d*)\s*/\s*(\d+)', rating_str)
    if rating_match:
        return float(rating_match.group(1))
    
    return None


def clean_colors(colors_str: str) -> int:
    """
    Clean and convert colors string to integer (numbers only)
    
    Args:
        colors_str (str): Colors string (e.g., "3", "5")
        
    Returns:
        int: Number of colors available
    """
    if not colors_str:
        return None
    
    try:
        return int(colors_str)
    except (ValueError, TypeError):
        return None


def standardize_size(size_str: str) -> str:
    """
    Standardize size values (size only, no "Size:" prefix)
    
    Args:
        size_str (str): Size string
        
    Returns:
        str: Standardized size or None if invalid
    """
    if not size_str or size_str == "Unknown":
        return None
    
    size_mapping = {
        "XS": "XS",
        "S": "S", 
        "M": "M",
        "L": "L",
        "XL": "XL",
        "XXL": "XXL"
    }
    
    clean_size = size_str.upper().strip()
    return size_mapping.get(clean_size, None)


def standardize_gender(gender_str: str) -> str:
    """
    Standardize gender values (gender only, no "Gender:" prefix)
    
    Args:
        gender_str (str): Gender string
        
    Returns:
        str: Standardized gender or None if invalid
    """
    if not gender_str or gender_str == "Unknown":
        return None
    
    gender_mapping = {
        "MEN": "Men",
        "WOMEN": "Women",
        "UNISEX": "Unisex"
    }
    
    clean_gender = gender_str.upper().strip()
    return gender_mapping.get(clean_gender, None)


def is_valid_title(title: str) -> bool:
    """
    Check if title is valid (not "Unknown Product")
    
    Args:
        title (str): Product title
        
    Returns:
        bool: True if valid, False otherwise
    """
    if not title:
        return False
    
    invalid_titles = ["unknown product", "unknown", ""]
    return title.lower().strip() not in invalid_titles


def transform_fashion_data(raw_data: List[Dict[str, str]]) -> pd.DataFrame:
    """
    Main transformation function to clean and process fashion data
    
    Args:
        raw_data (List[Dict[str, str]]): Raw extracted data
        
    Returns:
        pd.DataFrame: Transformed and cleaned data
    """
    if not raw_data:
        print("No data to transform")
        return pd.DataFrame()
    
    print(f"Transforming {len(raw_data)} products...")
    
    # Convert to DataFrame
    df = pd.DataFrame(raw_data)
    
    # Clean and transform each field
    df['price'] = df['price'].apply(clean_price)
    df['rating'] = df['rating'].apply(clean_rating)
    df['colors'] = df['colors'].apply(clean_colors)
    df['size'] = df['size'].apply(standardize_size)
    df['gender'] = df['gender'].apply(standardize_gender)
    
    # Filter out invalid data
    print("Filtering out invalid data...")
    
    # Remove rows with invalid titles
    df = df[df['title'].apply(is_valid_title)]
    
    # Remove rows with null/invalid values in critical fields
    df = df.dropna(subset=['title', 'price', 'rating', 'colors', 'size', 'gender'])
    
    # Remove rows where price is 0 (Price Unavailable)
    df = df[df['price'] > 0]
    
    # Remove duplicates based on title, price, size, and gender
    df = df.drop_duplicates(subset=['title', 'price', 'size', 'gender'], keep='first')
    
    # Reset index
    df = df.reset_index(drop=True)
    
    print(f"Transformation completed. Final dataset has {len(df)} products")
    print(f"Removed {len(raw_data) - len(df)} invalid/duplicate records")
    
    return df


if __name__ == "__main__":
    # Test transformation with sample data
    sample_data = [
        {
            'title': 'T-shirt 1',
            'price': '$102.15',
            'rating': '⭐ 3.9 / 5',
            'colors': '3',
            'size': 'M',
            'gender': 'Women',
            'timestamp': '2025-06-15 10:00:00'
        }
    ]
    
    transformed_df = transform_fashion_data(sample_data)
    print(transformed_df.head())