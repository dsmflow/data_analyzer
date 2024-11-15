import pandas as pd
import sqlalchemy as sa
from pathlib import Path
import logging
from typing import Optional, Dict, Any, Generator, Union
import gc
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataAnalyzer:
    def __init__(self, db_url: str = "sqlite:///data.db", chunk_size: int = 10000):
        """
        Initialize the DataAnalyzer with a database connection.
        
        Args:
            db_url: Database connection URL
            chunk_size: Number of rows to process at a time for large files
        """
        self.engine = sa.create_engine(db_url)
        self.metadata = sa.MetaData()
        self.chunk_size = chunk_size

    def read_csv_chunked(self, file_path: str, **kwargs) -> Generator[pd.DataFrame, None, None]:
        """
        Read a large CSV file in chunks to manage memory usage.
        
        Args:
            file_path: Path to the CSV file
            **kwargs: Additional arguments to pass to pd.read_csv
            
        Yields:
            pd.DataFrame: Chunks of the data
        """
        try:
            # First, get total number of lines for progress bar
            total_lines = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
            total_chunks = total_lines // self.chunk_size
            
            logger.info(f"Processing {file_path} in chunks of {self.chunk_size} rows")
            
            with tqdm(total=total_chunks, desc="Reading CSV") as pbar:
                for chunk in pd.read_csv(file_path, chunksize=self.chunk_size, **kwargs):
                    pbar.update(1)
                    yield chunk
                    
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            raise

    def analyze_sample(self, file_path: str, sample_size: int = 10000, **kwargs) -> Dict[str, Any]:
        """
        Analyze a sample of the data to understand its structure without loading the entire file.
        
        Args:
            file_path: Path to the CSV file
            sample_size: Number of rows to sample
            **kwargs: Additional arguments to pass to pd.read_csv
            
        Returns:
            Dict containing analysis results
        """
        try:
            logger.info(f"Analyzing sample of {sample_size} rows from {file_path}")
            sample_df = pd.read_csv(file_path, nrows=sample_size, **kwargs)
            
            analysis = {
                "sample_size": len(sample_df),
                "column_types": sample_df.dtypes.to_dict(),
                "missing_values": sample_df.isnull().sum().to_dict(),
                "memory_usage": {col: sample_df[col].memory_usage(deep=True) / 1024**2 for col in sample_df.columns},  # MB
                "sample_data": sample_df.head().to_dict()
            }
            
            # Estimate total file size
            total_lines = sum(1 for _ in open(file_path, 'r', encoding='utf-8')) - 1  # subtract header
            analysis["estimated_total_rows"] = total_lines
            analysis["estimated_memory_usage"] = sum(analysis["memory_usage"].values()) * (total_lines / sample_size)
            
            logger.info("Sample analysis complete")
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing sample: {str(e)}")
            raise

    def optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize DataFrame memory usage by choosing appropriate data types.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: Optimized DataFrame
        """
        for col in df.columns:
            if df[col].dtype == 'object':
                if df[col].nunique() / len(df) < 0.5:  # If column has low cardinality
                    df[col] = df[col].astype('category')
            elif df[col].dtype == 'int64':
                if df[col].min() >= -128 and df[col].max() <= 127:
                    df[col] = df[col].astype('int8')
                elif df[col].min() >= -32768 and df[col].max() <= 32767:
                    df[col] = df[col].astype('int16')
                elif df[col].min() >= -2147483648 and df[col].max() <= 2147483647:
                    df[col] = df[col].astype('int32')
        return df

    def save_to_db_chunked(self, file_path: str, table_name: str, if_exists: str = 'replace', **kwargs):
        """
        Save a large CSV file to database in chunks.
        
        Args:
            file_path: Path to the CSV file
            table_name: Name of the table in the database
            if_exists: How to behave if table exists ('fail', 'replace', or 'append')
            **kwargs: Additional arguments to pass to pd.read_csv
        """
        first_chunk = True
        total_rows = 0
        
        try:
            for chunk in self.read_csv_chunked(file_path, **kwargs):
                # Optimize memory usage
                chunk = self.optimize_dtypes(chunk)
                
                # Save chunk to database
                chunk.to_sql(
                    table_name,
                    self.engine,
                    if_exists='replace' if first_chunk else 'append',
                    index=False
                )
                
                total_rows += len(chunk)
                first_chunk = False
                
                # Force garbage collection
                del chunk
                gc.collect()
            
            logger.info(f"Successfully saved {total_rows} rows to table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error saving to database: {str(e)}")
            raise

    def query_data(self, query: str, chunksize: Optional[int] = None) -> Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]:
        """
        Execute a SQL query on the database, optionally in chunks.
        
        Args:
            query: SQL query string
            chunksize: If specified, return an iterator for processing large results
            
        Returns:
            Either a DataFrame or an iterator of DataFrames
        """
        try:
            logger.info(f"Executing query: {query}")
            if chunksize:
                return pd.read_sql_query(query, self.engine, chunksize=chunksize)
            return pd.read_sql_query(query, self.engine)
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

def main():
    # Example usage for large files
    analyzer = DataAnalyzer(chunk_size=50000)
    
    # Example usage:
    # 1. Analyze sample first
    # analysis = analyzer.analyze_sample("large_dataset.csv")
    # print(f"Estimated memory usage: {analysis['estimated_memory_usage']:.2f} MB")
    
    # 2. Process and save to database in chunks
    # analyzer.save_to_db_chunked("large_dataset.csv", "my_table")
    
    # 3. Query in chunks if needed
    # for chunk in analyzer.query_data("SELECT * FROM my_table", chunksize=10000):
    #     process_chunk(chunk)
    
    logger.info("DataAnalyzer initialized and ready to use")

if __name__ == "__main__":
    main()
