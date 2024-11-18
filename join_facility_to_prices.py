import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime
import sys
from memory_profiler import memory_usage
import time

# Setup logging
log_path = "/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/price_transparency_files/austin_mrfs/austin_cigna_oap/unfiltered/processing_log.txt"
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log_memory_usage():
    """Log current memory usage"""
    mem = memory_usage(-1, interval=.1, timeout=1)[0]
    logging.info(f"Current memory usage: {mem:.2f} MiB")

def main():
    start_time = time.time()
    logging.info("Starting data processing")
    
    # File paths
    ppo_file = "/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/price_transparency_files/austin_mrfs/austin_cigna_oap/unfiltered/austin_cigna_oap_unfiltered_austin_cigna_oap_prices_combined.csv"
    taxonomy_file = "/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/price_transparency_files/austin_mrfs/austin_unfiltered/taxonomy_filtered_facilities.csv"
    output_file = "/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/price_transparency_files/austin_mrfs/austin_cigna_oap/unfiltered/final_combined_table.csv"
    
    # Create SQLite database
    db_path = 'temp_join.db'
    conn = sqlite3.connect(db_path)
    logging.info("Created temporary SQLite database")
    
    try:
        # Process PPO file in chunks
        chunk_size = 50000
        total_ppo_rows = 0
        logging.info("Starting PPO file processing")
        
        for chunk_num, chunk in enumerate(pd.read_csv(ppo_file, chunksize=chunk_size, dtype={'npi': 'Int64'})):
            # Clean NPI values
            chunk['npi'] = pd.to_numeric(chunk['npi'], errors='coerce')
            chunk = chunk.dropna(subset=['npi'])
            chunk['npi'] = chunk['npi'].astype('Int64')
            
            # Write to SQLite
            chunk.to_sql('ppo_table', conn, if_exists='append' if chunk_num > 0 else 'replace', index=False)
            total_ppo_rows += len(chunk)
            logging.info(f"Processed PPO chunk {chunk_num + 1}, Total rows so far: {total_ppo_rows}")
            log_memory_usage()
        
        # Process taxonomy file
        logging.info("Processing taxonomy file")
        taxonomy_df = pd.read_csv(taxonomy_file, dtype={'NPI': 'Int64'})
        taxonomy_df['NPI'] = pd.to_numeric(taxonomy_df['NPI'], errors='coerce')
        taxonomy_df = taxonomy_df.dropna(subset=['NPI'])
        taxonomy_df.to_sql('taxonomy_table', conn, if_exists='replace', index=False)
        logging.info(f"Processed taxonomy file, Total rows: {len(taxonomy_df)}")
        
        # Create indices for faster joining
        logging.info("Creating database indices")
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ppo_npi ON ppo_table(npi)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_tax_npi ON taxonomy_table(NPI)')
        
        # Perform the join in chunks
        logging.info("Starting join operation")
        query = '''
        SELECT p.*, 
               t.*
        FROM ppo_table p
        LEFT JOIN taxonomy_table t ON p.npi = t.NPI
        '''
        
        # Write header first
        first_chunk = True
        total_output_rows = 0
        
        for chunk_df in pd.read_sql(query, conn, chunksize=chunk_size):
            mode = 'w' if first_chunk else 'a'
            chunk_df.to_csv(output_file, mode=mode, index=False, header=first_chunk)
            first_chunk = False
            total_output_rows += len(chunk_df)
            logging.info(f"Wrote chunk to output file, Total rows written: {total_output_rows}")
            log_memory_usage()
        
        end_time = time.time()
        logging.info(f"Processing completed successfully")
        logging.info(f"Total execution time: {(end_time - start_time):.2f} seconds")
        logging.info(f"Total input PPO rows: {total_ppo_rows}")
        logging.info(f"Total input taxonomy rows: {len(taxonomy_df)}")
        logging.info(f"Total output rows: {total_output_rows}")
        
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}", exc_info=True)
        raise
    
    finally:
        # Clean up
        conn.close()
        if os.path.exists(db_path):
            os.remove(db_path)
            logging.info("Cleaned up temporary database")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("Fatal error in main program", exc_info=True)
        sys.exit(1)