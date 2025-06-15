"""
Unit tests for the load module
"""

import pytest
import pandas as pd
import os
import tempfile
import sys

# Add utils directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from utils.load import (
    validate_dataframe, save_to_csv, append_to_csv, 
    generate_summary_report, load_fashion_data
)


class TestLoad:
    
    def test_validate_dataframe_valid(self):
        """Test dataframe validation with valid dataframe"""
        df = pd.DataFrame({
            'title': ['Product 1'],
            'price': [415840.0],
            'rating': [4.5],
            'colors': [3],
            'size': ['M'],
            'gender': ['Unisex'],
            'timestamp': ['2025-06-15 10:00:00']
        })
        
        assert validate_dataframe(df) == True
    
    def test_validate_dataframe_empty(self):
        """Test dataframe validation with empty dataframe"""
        df = pd.DataFrame()
        assert validate_dataframe(df) == False
    
    def test_validate_dataframe_none(self):
        """Test dataframe validation with None"""
        assert validate_dataframe(None) == False
    
    def test_validate_dataframe_missing_columns(self):
        """Test dataframe validation with missing required columns"""
        df = pd.DataFrame({
            'title': ['Product 1'],
            'price': [25.99]
            # Missing required columns: rating, colors, size, gender, timestamp
        })
        
        assert validate_dataframe(df) == False
    
    def test_save_to_csv_success(self):
        """Test successful CSV saving"""
        df = pd.DataFrame({
            'title': ['Product 1', 'Product 2'],
            'price': [415840.0, 735840.0],
            'rating': [4.5, 3.8],
            'colors': [3, 2],
            'size': ['M', 'L'],
            'gender': ['Men', 'Women'],
            'timestamp': ['2025-06-15 10:00:00', '2025-06-15 10:00:00']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_file:
            tmp_path = tmp_file.name
        
        try:
            result = save_to_csv(df, tmp_path, create_backup_flag=False)
            assert result == True
            
            # Verify file was created and contains correct data
            assert os.path.exists(tmp_path)
            loaded_df = pd.read_csv(tmp_path)
            assert len(loaded_df) == 2
            assert list(loaded_df['title']) == ['Product 1', 'Product 2']
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_save_to_csv_invalid_dataframe(self):
        """Test CSV saving with invalid dataframe"""
        df = pd.DataFrame()  # Empty dataframe
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_file:
            tmp_path = tmp_file.name
        
        try:
            result = save_to_csv(df, tmp_path)
            assert result == False
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_append_to_csv_new_file(self):
        """Test appending to CSV when file doesn't exist"""
        df = pd.DataFrame({
            'title': ['Product 1'],
            'price': [415840.0],
            'rating': [4.5],
            'colors': [3],
            'size': ['M'],
            'gender': ['Men'],
            'timestamp': ['2025-06-15 10:00:00']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_file:
            tmp_path = tmp_file.name
        
        # Remove the file so it doesn't exist
        os.unlink(tmp_path)
        
        try:
            result = append_to_csv(df, tmp_path)
            assert result == True
            
            # Verify file was created
            assert os.path.exists(tmp_path)
            loaded_df = pd.read_csv(tmp_path)
            assert len(loaded_df) == 1
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_append_to_csv_existing_file(self):
        """Test appending to existing CSV file"""
        # Create initial dataframe and save it
        initial_df = pd.DataFrame({
            'title': ['Product 1'],
            'price': [415840.0],
            'rating': [4.5],
            'colors': [3],
            'size': ['M'],
            'gender': ['Men'],
            'timestamp': ['2025-06-15 10:00:00']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_file:
            tmp_path = tmp_file.name
        
        try:
            # Save initial data
            initial_df.to_csv(tmp_path, index=False)
            
            # Create new data to append
            new_df = pd.DataFrame({
                'title': ['Product 2'],
                'price': [735840.0],
                'rating': [3.8],
                'colors': [2],
                'size': ['L'],
                'gender': ['Women'],
                'timestamp': ['2025-06-15 10:00:00']
            })
            
            # Append new data
            result = append_to_csv(new_df, tmp_path)
            assert result == True
            
            # Verify combined data
            loaded_df = pd.read_csv(tmp_path)
            assert len(loaded_df) == 2
            assert 'Product 1' in loaded_df['title'].values
            assert 'Product 2' in loaded_df['title'].values
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_generate_summary_report(self):
        """Test summary report generation"""
        df = pd.DataFrame({
            'title': ['T-shirt 1', 'Hoodie 1', 'T-shirt 2'],
            'price': [415840.0, 735840.0, 495840.0],
            'rating': [4.5, 3.8, 4.2],
            'colors': [3, 2, 3],
            'size': ['M', 'L', 'M'],
            'gender': ['Men', 'Women', 'Men'],
            'timestamp': ['2025-06-15 10:00:00', '2025-06-15 10:00:00', '2025-06-15 10:00:00']
        })
        
        summary = generate_summary_report(df)
        
        assert summary['total_products'] == 3
        assert 'price_stats' in summary
        assert 'avg_price' in summary['price_stats']
        assert 'rating_stats' in summary
        assert 'avg_rating' in summary['rating_stats']
    
    def test_generate_summary_report_empty(self):
        """Test summary report generation with empty dataframe"""
        df = pd.DataFrame()
        summary = generate_summary_report(df)
        assert summary == {}
    
    def test_load_fashion_data_overwrite_mode(self):
        """Test main load function in overwrite mode"""
        df = pd.DataFrame({
            'title': ['Product 1'],
            'price': [415840.0],
            'rating': [4.5],
            'colors': [3],
            'size': ['M'],
            'gender': ['Men'],
            'timestamp': ['2025-06-15 10:00:00']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_file:
            tmp_path = tmp_file.name
        
        try:
            result = load_fashion_data(df, tmp_path, mode="overwrite")
            assert result == True
            
            # Verify file exists and contains data
            assert os.path.exists(tmp_path)
            loaded_df = pd.read_csv(tmp_path)
            assert len(loaded_df) == 1
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_load_fashion_data_append_mode(self):
        """Test main load function in append mode"""
        df = pd.DataFrame({
            'title': ['Product 1'],
            'price': [415840.0],
            'rating': [4.5],
            'colors': [3],
            'size': ['M'],
            'gender': ['Men'],
            'timestamp': ['2025-06-15 10:00:00']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_file:
            tmp_path = tmp_file.name
        
        # Remove the empty file created by NamedTemporaryFile
        os.unlink(tmp_path)
        
        try:
            result = load_fashion_data(df, tmp_path, mode="append")
            assert result == True
            
            # Verify file exists and contains data
            assert os.path.exists(tmp_path)
            loaded_df = pd.read_csv(tmp_path)
            assert len(loaded_df) == 1
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


if __name__ == "__main__":
    pytest.main([__file__])