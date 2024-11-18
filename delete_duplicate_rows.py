import os
import pandas as pd

def remove_duplicates_in_csv(folder_path):
    # Loop through each file in the specified folder
    for filename in os.listdir(folder_path):
        # Process only CSV files
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)
            
            # Count the number of duplicates before removing
            initial_row_count = len(df)
            df.drop_duplicates(inplace=True)
            final_row_count = len(df)
            
            # Calculate the number of duplicates removed
            duplicates_removed = initial_row_count - final_row_count
            
            # Save the changes back to the same file
            df.to_csv(file_path, index=False)
            
            print(f"Duplicates removed in file '{filename}': {duplicates_removed}")

# Specify the folder containing your CSV files
folder_path = '/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/price_transparency_files/austin_mrfs/austin_cigna_oap/unfiltered'
remove_duplicates_in_csv(folder_path)
