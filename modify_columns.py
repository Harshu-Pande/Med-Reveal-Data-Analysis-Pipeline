import pandas as pd
import os

# Function to process the main table with logging
def process_table(main_table_path, lookup_table_path):
    # Step 0: Load the main table
    print("Loading main table...")
    main_df = pd.read_csv(main_table_path)
    print(f"Main table loaded. Shape: {main_df.shape}")

    # Step 1: Load the lookup table with procedure names
    print("Loading lookup table...")
    lookup_df = pd.read_csv(lookup_table_path)
    print(f"Lookup table loaded. Shape: {lookup_df.shape}")

    # Create a dictionary for billing code-to-description mapping
    billing_code_to_description = lookup_df.set_index('billing_code')['plain_language_description'].to_dict()

    # Step 2: Remove 'Other Provider Identifier' columns
    print("Removing 'Other Provider Identifier' columns...")
    other_provider_columns = [
        'Other Provider Identifier_1', 'Other Provider Identifier Type Code_1', 'Other Provider Identifier State_1', 'Other Provider Identifier Issuer_1',
        'Other Provider Identifier_2', 'Other Provider Identifier Type Code_2', 'Other Provider Identifier State_2', 'Other Provider Identifier Issuer_2',
        'Other Provider Identifier_3', 'Other Provider Identifier Type Code_3', 'Other Provider Identifier State_3', 'Other Provider Identifier Issuer_3',
        'Other Provider Identifier_4', 'Other Provider Identifier Type Code_4', 'Other Provider Identifier State_4', 'Other Provider Identifier Issuer_4',
        'Other Provider Identifier_5', 'Other Provider Identifier Type Code_5', 'Other Provider Identifier State_5', 'Other Provider Identifier Issuer_5',
        'Other Provider Identifier_6', 'Other Provider Identifier Type Code_6', 'Other Provider Identifier State_6', 'Other Provider Identifier Issuer_6',
        'Other Provider Identifier_7', 'Other Provider Identifier Type Code_7', 'Other Provider Identifier State_7', 'Other Provider Identifier Issuer_7',
        'Other Provider Identifier_8', 'Other Provider Identifier Type Code_8', 'Other Provider Identifier State_8', 'Other Provider Identifier Issuer_8',
        'Other Provider Identifier_9', 'Other Provider Identifier Type Code_9', 'Other Provider Identifier State_9', 'Other Provider Identifier Issuer_9'
    ]
    main_df.drop(columns=other_provider_columns, inplace=True, errors='ignore')
    print("Other Provider Identifier columns removed.")

    # Step 3: Create 'Combined_taxonomy_codes' column by combining taxonomy code columns
    print("Combining taxonomy code columns into 'Combined_taxonomy_codes'...")
    taxonomy_columns = [
        'Healthcare Provider Taxonomy Code_1', 'Healthcare Provider Taxonomy Code_2',
        'Healthcare Provider Taxonomy Code_3', 'Healthcare Provider Taxonomy Code_4',
        'Healthcare Provider Taxonomy Code_5', 'Healthcare Provider Taxonomy Code_6',
        'Healthcare Provider Taxonomy Code_7', 'Healthcare Provider Taxonomy Code_8',
        'Healthcare Provider Taxonomy Code_9', 'Healthcare Provider Taxonomy Code_10',
        'Healthcare Provider Taxonomy Code_11', 'Healthcare Provider Taxonomy Code_12',
        'Healthcare Provider Taxonomy Code_13', 'Healthcare Provider Taxonomy Code_14',
        'Healthcare Provider Taxonomy Code_15'
    ]
    main_df['Combined_taxonomy_codes'] = main_df[taxonomy_columns].apply(lambda row: ', '.join(row.dropna().astype(str)), axis=1)
    print("Combined_taxonomy_codes column created.")

    # Step 4: Remove original taxonomy columns
    main_df.drop(columns=taxonomy_columns, inplace=True)
    print("Original taxonomy columns removed.")

    # Step 5: Add 'procedure_name' column by mapping billing codes to descriptions from the lookup table
    print("Mapping 'billing_code' to 'procedure_name' using lookup table...")
    main_df.insert(0, 'procedure_name', main_df['billing_code'].map(billing_code_to_description))
    print("procedure_name column added.")

    # Step 6: Remove rows where 'procedure_name' is blank (NaN)
    initial_shape = main_df.shape
    main_df.dropna(subset=['procedure_name'], inplace=True)
    print(f"Removed rows with empty 'procedure_name'. Rows removed: {initial_shape[0] - main_df.shape[0]}")

    # Step 7: Remove rows where 'billing_code_modifier' is NOT empty
    print("Removing rows where 'billing_code_modifier' is NOT empty...")
    initial_shape = main_df.shape
    main_df = main_df[main_df['billing_code_modifier'].isna()]
    print(f"Rows removed based on 'billing_code_modifier': {initial_shape[0] - main_df.shape[0]}")

    # Step 8: Drop the 'billing_code_modifier' column
    main_df.drop(columns=['billing_code_modifier'], inplace=True)
    print("'billing_code_modifier' column removed.")

    # Step 9: Save the modified table to a new CSV in the same directory
    output_file_path = os.path.join(os.path.dirname(main_table_path), "final_processed_table.csv")
    main_df.to_csv(output_file_path, index=False)
    print(f"Processed table saved to: {output_file_path}")

# Paths for the main and lookup tables
main_table_path = "/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/price_transparency_files/austin_mrfs/austin_cigna_oap/unfiltered/final_combined_table.csv"
lookup_table_path = "/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/price_transparency_files/austin_mrfs/austin_code_filtered/tic_500_shoppable_services.csv"

# Run the function
process_table(main_table_path, lookup_table_path)
