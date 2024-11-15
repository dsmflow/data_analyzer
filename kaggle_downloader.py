import os
from pathlib import Path
import logging
from kaggle.api.kaggle_api_extended import KaggleApi
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KaggleDownloader:
    def __init__(self, data_dir: str = "datasets"):
        """
        Initialize KaggleDownloader with a target directory for datasets.
        
        Args:
            data_dir: Directory where datasets will be stored
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.api = KaggleApi()
        self.api.authenticate()

    def download_dataset(self, dataset_name: str, unzip: bool = True) -> Path:
        """
        Download a dataset from Kaggle.
        
        Args:
            dataset_name: Name of dataset in format 'owner/dataset-name'
            unzip: Whether to unzip the downloaded file
            
        Returns:
            Path to the downloaded dataset
        """
        try:
            logger.info(f"Downloading dataset: {dataset_name}")
            
            # Create specific directory for this dataset
            dataset_dir = self.data_dir / dataset_name.split('/')[1]
            dataset_dir.mkdir(parents=True, exist_ok=True)
            
            # Download the dataset
            self.api.dataset_download_files(
                dataset_name,
                path=str(dataset_dir),
                unzip=unzip,
                quiet=False  # Show progress
            )
            
            logger.info(f"Dataset downloaded to: {dataset_dir}")
            return dataset_dir

        except Exception as e:
            logger.error(f"Error downloading dataset: {str(e)}")
            raise

    def list_files(self, dataset_dir: Path) -> list:
        """
        List all CSV files in the dataset directory.
        
        Args:
            dataset_dir: Path to dataset directory
            
        Returns:
            List of CSV file paths
        """
        try:
            csv_files = list(dataset_dir.glob("**/*.csv"))
            logger.info(f"Found {len(csv_files)} CSV files in {dataset_dir}")
            return csv_files
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            raise

def main():
    # Example usage
    downloader = KaggleDownloader()
    
    # Example dataset (uncomment and modify as needed):
    # dataset_name = "your-dataset-owner/your-dataset-name"
    # dataset_dir = downloader.download_dataset(dataset_name)
    # csv_files = downloader.list_files(dataset_dir)
    # print(f"Available CSV files: {csv_files}")

if __name__ == "__main__":
    main()
