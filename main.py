"""
Main ETL Pipeline for Fashion Studio Data
This script orchestrates the Extract, Transform, and Load processes
"""

import sys
import os
from datetime import datetime

# Add utils directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from utils.extract import extract_fashion_data
from utils.transform import transform_fashion_data
from utils.load import load_fashion_data


def run_etl_pipeline(url: str = "https://fashion-studio.dicoding.dev/", 
                    output_file: str = "products.csv",
                    mode: str = "overwrite",
                    max_pages: int = 50) -> bool:
    """
    Run the complete ETL pipeline
    
    Args:
        url (str): URL to extract data from
        output_file (str): Output CSV file path
        mode (str): Loading mode - 'overwrite' or 'append'
        max_pages (int): Maximum number of pages to scrape
        
    Returns:
        bool: True if pipeline completed successfully, False otherwise
    """
    print("=" * 60)
    print("FASHION STUDIO ETL PIPELINE")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Source URL: {url}")
    print(f"Output file: {output_file}")
    print(f"Mode: {mode}")
    print("-" * 60)
    
    try:
        # EXTRACT PHASE
        print("\n🔍 EXTRACT PHASE")
        print("-" * 20)
        raw_data = extract_fashion_data(url, max_pages)
        
        if not raw_data:
            print("❌ Extract phase failed: No data extracted")
            return False
        
        print(f"✅ Extract phase completed: {len(raw_data)} products extracted")
        
        # TRANSFORM PHASE
        print("\n🔄 TRANSFORM PHASE")
        print("-" * 20)
        transformed_df = transform_fashion_data(raw_data)
        
        if transformed_df.empty:
            print("❌ Transform phase failed: No data to transform")
            return False
        
        print(f"✅ Transform phase completed: {len(transformed_df)} products transformed")
        
        # LOAD PHASE
        print("\n💾 LOAD PHASE")
        print("-" * 20)
        load_success = load_fashion_data(transformed_df, output_file, mode)
        
        if not load_success:
            print("❌ Load phase failed")
            return False
        
        print("✅ Load phase completed")
        
        # PIPELINE SUMMARY
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total products processed: {len(transformed_df)}")
        print(f"Output saved to: {output_file}")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {str(e)}")
        print("=" * 60)
        return False


def main():
    """
    Main function to run the ETL pipeline with command line arguments
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Fashion Studio ETL Pipeline')
    parser.add_argument('--url', 
                       default='https://fashion-studio.dicoding.dev/',
                       help='URL to extract data from')
    parser.add_argument('--output', 
                       default='products.csv',
                       help='Output CSV file path')
    parser.add_argument('--mode', 
                       choices=['overwrite', 'append'],
                       default='overwrite',
                       help='Loading mode: overwrite or append')
    parser.add_argument('--max-pages', 
                       type=int,
                       default=50,
                       help='Maximum number of pages to scrape (1-50)')
    
    args = parser.parse_args()
    
    # Run the ETL pipeline
    success = run_etl_pipeline(
        url=args.url,
        output_file=args.output,
        mode=args.mode,
        max_pages=args.max_pages
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()