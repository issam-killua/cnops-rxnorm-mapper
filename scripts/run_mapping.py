#!/usr/bin/env python3
'''
Main execution script for CNOPS to RxNorm mapping
'''

import os
import sys
import argparse
import logging
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from mapper.core_mapper import CNOPSToRxNormMapper

def setup_logging():
    '''Setup logging configuration'''
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/mapping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )

def main():
    parser = argparse.ArgumentParser(description='Map CNOPS drugs to RxNorm')
    parser.add_argument('--input', '-i', 
                       default='data/input/refdesmedicamentscnops2014.xlsx',
                       help='Input CNOPS Excel file')
    parser.add_argument('--output', '-o',
                       default='data/output/cnops_rxnorm_mappings.xlsx',
                       help='Output Excel file')
    parser.add_argument('--config', '-c',
                       default='config/mapping_config.yaml',
                       help='Mapping configuration file')
    
    args = parser.parse_args()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting CNOPS to RxNorm mapping process")
    logger.info(f"Input file: {args.input}")
    logger.info(f"Output file: {args.output}")
    
    try:
        # Check if input file exists
        if not os.path.exists(args.input):
            logger.error(f"Input file not found: {args.input}")
            return 1
        
        # Create output directory if needed
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        
        # Initialize mapper
        mapper = CNOPSToRxNormMapper(args.config)
        
        # Process file
        results_df = mapper.process_file(args.input, args.output)
        
        logger.info("Mapping process completed successfully!")
        print(f"\nResults saved to: {args.output}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Mapping process failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
