from kaggle_downloader import KaggleDownloader
from data_analyzer import DataAnalyzer
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Initialize our classes
    downloader = KaggleDownloader(data_dir="stock_data")
    analyzer = DataAnalyzer(chunk_size=100000)  # Larger chunk size for stock data
    
    # Download the stock market dataset
    dataset_name = "jakewright/9000-tickers-of-stock-market-data-full-history"
    try:
        dataset_dir = downloader.download_dataset(dataset_name)
        csv_files = downloader.list_files(dataset_dir)
        
        if not csv_files:
            logger.error("No CSV files found in the dataset")
            return
            
        # Analyze the first CSV file as a sample
        first_file = csv_files[0]
        logger.info(f"Analyzing sample from: {first_file}")
        
        # Get sample analysis
        analysis = analyzer.analyze_sample(str(first_file), sample_size=50000)
        
        # Print analysis results
        print("\nDataset Analysis:")
        print(f"Estimated total rows: {analysis['estimated_total_rows']:,}")
        print(f"Estimated memory usage: {analysis['estimated_memory_usage']:.2f} MB")
        print("\nColumn Types:")
        for col, dtype in analysis['column_types'].items():
            print(f"- {col}: {dtype}")
        
        print("\nMissing Values:")
        for col, missing in analysis['missing_values'].items():
            if missing > 0:
                print(f"- {col}: {missing:,} missing values")
        
        # Import data into SQLite database
        print("\nImporting data into database (this may take a while)...")
        table_name = "stock_market_data"
        logger.info(f"Importing data into table: {table_name}")
        analyzer.save_to_db_chunked(str(first_file), table_name)
        
        # Show sample query results
        print("\nData imported successfully! Here's a sample of the data:")
        sample = analyzer.query_data("SELECT * FROM stock_market_data LIMIT 5")
        print("\nFirst 5 rows:")
        print(sample)
        
        count = analyzer.query_data("SELECT COUNT(*) as total FROM stock_market_data")
        print(f"\nTotal rows in database: {count.iloc[0]['total']:,}")
        
        symbols = analyzer.query_data("""
            SELECT Ticker, COUNT(*) as count 
            FROM stock_market_data 
            GROUP BY Ticker 
            ORDER BY count DESC 
            LIMIT 5
        """)
        print("\nTop 5 tickers by number of records:")
        print(symbols)
        
        # Show some basic statistics
        stats = analyzer.query_data("""
            SELECT 
                Ticker,
                MIN(Date) as first_date,
                MAX(Date) as last_date,
                COUNT(*) as days,
                AVG(Close) as avg_price,
                MAX(Close) as highest_price,
                MIN(Close) as lowest_price
            FROM stock_market_data
            GROUP BY Ticker
            ORDER BY days DESC
            LIMIT 5
        """)
        print("\nMost complete historical records:")
        print(stats)
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
