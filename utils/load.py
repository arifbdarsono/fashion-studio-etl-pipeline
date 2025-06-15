"""
Load module for Fashion Studio ETL Pipeline
This module handles data loading and storage operations
"""

import pandas as pd
import os
from typing import Optional
from datetime import datetime


def validate_dataframe(df: pd.DataFrame) -> bool:
    """
    Validate dataframe before saving
    
    Args:
        df (pd.DataFrame): DataFrame to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    if df is None or df.empty:
        print("Error: DataFrame is empty or None")
        return False
    
    required_columns = ['title', 'price', 'rating', 'colors', 'size', 'gender']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Error: Missing required columns: {missing_columns}")
        return False
    
    return True


def create_backup(file_path: str) -> bool:
    """
    Create backup of existing file
    
    Args:
        file_path (str): Path to the file to backup
        
    Returns:
        bool: True if backup created successfully, False otherwise
    """
    if not os.path.exists(file_path):
        return True  # No file to backup
    
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{file_path}.backup_{timestamp}"
        
        # Read and write to create backup
        df = pd.read_csv(file_path)
        df.to_csv(backup_path, index=False)
        
        print(f"Backup created: {backup_path}")
        return True
    except Exception as e:
        print(f"Warning: Could not create backup: {e}")
        return False


def save_to_csv(df: pd.DataFrame, file_path: str, create_backup_flag: bool = True) -> bool:
    """
    Save DataFrame to CSV file
    
    Args:
        df (pd.DataFrame): DataFrame to save
        file_path (str): Path where to save the CSV file
        create_backup_flag (bool): Whether to create backup of existing file
        
    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        # Validate DataFrame
        if not validate_dataframe(df):
            return False
        
        # Create backup if requested and file exists
        if create_backup_flag:
            create_backup(file_path)
        
        # Ensure directory exists
        dir_path = os.path.dirname(file_path)
        if dir_path:  # Only create directory if there is a directory path
            os.makedirs(dir_path, exist_ok=True)
        
        # Save to CSV
        df.to_csv(file_path, index=False, encoding='utf-8')
        
        print(f"Data successfully saved to: {file_path}")
        print(f"Total records saved: {len(df)}")
        
        return True
        
    except Exception as e:
        print(f"Error saving data to CSV: {e}")
        return False


def append_to_csv(df: pd.DataFrame, file_path: str) -> bool:
    """
    Append DataFrame to existing CSV file
    
    Args:
        df (pd.DataFrame): DataFrame to append
        file_path (str): Path to the CSV file
        
    Returns:
        bool: True if appended successfully, False otherwise
    """
    try:
        # Validate DataFrame
        if not validate_dataframe(df):
            return False
        
        if os.path.exists(file_path):
            # Read existing data
            existing_df = pd.read_csv(file_path)
            
            # Combine data
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            
            # Remove duplicates based on key columns
            combined_df = combined_df.drop_duplicates(
                subset=['title', 'price', 'size', 'gender'], 
                keep='last'
            )
            
            # Save combined data
            combined_df.to_csv(file_path, index=False, encoding='utf-8')
            
            print(f"Data appended to: {file_path}")
            print(f"Total records after append: {len(combined_df)}")
            
        else:
            # File doesn't exist, create new
            return save_to_csv(df, file_path, create_backup_flag=False)
        
        return True
        
    except Exception as e:
        print(f"Error appending data to CSV: {e}")
        return False


def generate_summary_report(df: pd.DataFrame) -> dict:
    """
    Generate summary report of the loaded data
    
    Args:
        df (pd.DataFrame): DataFrame to analyze
        
    Returns:
        dict: Summary statistics
    """
    if df.empty:
        return {}
    
    summary = {
        'total_products': len(df),
        'price_stats': {
            'min_price': df['price'].min() if 'price' in df.columns else 0,
            'max_price': df['price'].max() if 'price' in df.columns else 0,
            'avg_price': df['price'].mean() if 'price' in df.columns else 0
        },
        'rating_stats': {
            'min_rating': df['rating'].min() if 'rating' in df.columns else 0,
            'max_rating': df['rating'].max() if 'rating' in df.columns else 0,
            'avg_rating': df['rating'].mean() if 'rating' in df.columns else 0
        },
        'gender_distribution': df['gender'].value_counts().to_dict() if 'gender' in df.columns else {},
        'size_distribution': df['size'].value_counts().to_dict() if 'size' in df.columns else {},
        'colors_stats': {
            'min_colors': df['colors'].min() if 'colors' in df.columns else 0,
            'max_colors': df['colors'].max() if 'colors' in df.columns else 0,
            'avg_colors': df['colors'].mean() if 'colors' in df.columns else 0
        }
    }
    
    return summary


def load_fashion_data(df: pd.DataFrame, output_path: str = "products.csv", 
                     mode: str = "overwrite") -> bool:
    """
    Main loading function to save fashion data
    
    Args:
        df (pd.DataFrame): Transformed data to load
        output_path (str): Output file path
        mode (str): Loading mode - 'overwrite' or 'append'
        
    Returns:
        bool: True if loaded successfully, False otherwise
    """
    print(f"Loading data to: {output_path}")
    print(f"Mode: {mode}")
    
    if mode == "append":
        success = append_to_csv(df, output_path)
    else:
        success = save_to_csv(df, output_path)
    
    if success:
        # Generate and print summary report
        summary = generate_summary_report(df)
        print("\n=== DATA LOADING SUMMARY ===")
        print(f"Total products loaded: {summary.get('total_products', 0)}")
        
        if summary.get('price_stats'):
            price_stats = summary['price_stats']
            print(f"Price range (IDR): {price_stats['min_price']:,.0f} - {price_stats['max_price']:,.0f}")
            print(f"Average price (IDR): {price_stats['avg_price']:,.0f}")
        
        if summary.get('rating_stats'):
            rating_stats = summary['rating_stats']
            print(f"Rating range: {rating_stats['min_rating']:.1f} - {rating_stats['max_rating']:.1f}")
            print(f"Average rating: {rating_stats['avg_rating']:.1f}")
        
        if summary.get('gender_distribution'):
            print("Gender distribution:")
            for gender, count in summary['gender_distribution'].items():
                print(f"  - {gender}: {count}")
        
        if summary.get('size_distribution'):
            print("Size distribution:")
            for size, count in summary['size_distribution'].items():
                print(f"  - {size}: {count}")
        
        print("=== END SUMMARY ===\n")
    
    return success


if __name__ == "__main__":
    # Test loading with sample data
    sample_df = pd.DataFrame([
        {
            'title': 'Test Product',
            'price': 415840.0,  # IDR
            'rating': 4.5,
            'colors': 3,
            'size': 'M',
            'gender': 'Unisex',
            'timestamp': '2025-06-15 10:00:00'
        }
    ])
    
    success = load_fashion_data(sample_df, "test_products.csv")
    print(f"Test loading result: {success}")