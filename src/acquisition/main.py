import sys
from .pipeline import main as pipeline_main

def main():
    """
    Main entry point for the Qualitative Data Scraper Pipeline.
    This delegates to the new pipeline orchestrator.
    """
    # The pipeline script handles argument parsing and logging setup
    pipeline_main()

if __name__ == "__main__":
    main()
