# Data Analysis Pipeline

This document describes the pipeline used for analyzing price transparency data provided by health insurance companies.

## **1. Data Ingestion Pipeline**

### **Finding URLs of Price Transparency Files**
The data ingestion begins by identifying URLs of price transparency files provided by major health insurance companies. There are four primary health insurance providers that cover most of the US population:  
- **Blue Cross Blue Shield (BCBS)**  
- **United Healthcare (UHC)**  
- **Cigna**  
- **Aetna**
---

For this example, I will be using **Aetna's** insurance plan for Texas which can be found at the url `https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICFI/machine-readable-transparency-in-coverage`

---
### **JSON to CSV Conversion**
These URLs link to gzipped JSON files containing pricing data for specific insurance plans. These files are processed using a custom JSON parser that converts the data into CSV files with a predefined schema.

**Preferred Environment:**  
Google Colab is used for processing to save local disk space.

#### **Steps for JSON to CSV Conversion**
1. Clone the repository:
   ```bash
   !git clone https://github.com/dolthub/data-analysis.git
   ```
2. Install the required packages:
   ```bash
   cd /content/data-analysis/transparency-in-coverage/python/mrfutils
   pip install .
   ```
3. Run the script to extract CSV data:
   ```bash
   !python3 examples/example_cli.py \
   --out-dir /content/drive/MyDrive/price_transparency_files/austin_mrfs/austin_aetna/unfiltered \
   --code-file /content/data-analysis/transparency-in-coverage/python/mrfutils/data/hpt/70_shoppables.csv \
   --npi-file /content/drive/MyDrive/price_transparency_files/austin_mrfs/austin_taxonomy_npi_mrfutils_facilities.csv \
   --url 'https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICFI/2024-10-05/inNetworkRates/2024-10-05_pl-3vk-hr23_Aetna-Health-Inc.---Texas.json.gz' \
   --file "/content/unzipped_aetna_hmo.json"
   ```

**Flags and Parameters:**  
- `--code-file`: Specifies the procedures of interest (e.g., surgical, imaging, medications).  
- `--npi-file`: Specifies the hospitals/providers of interest.  
- `--url`: URL of the price transparency file.  
- `--file`: Optionally, include the downloaded JSON file for direct parsing.

**Outputs:**  
The script generates the following 7 CSV files:
- `Code.csv`  
- `File.csv`  
- `Npi_tin.csv`  
- `Rate_metadata.csv`  
- `Rate.csv`  
- `Tin_rate_file.csv`  
- `Tin_csv`

---

## **2. Combining and Cleaning CSV Data**

### **Step-by-Step Instructions**
1. **Prepare the Data:**  
   Save all 7 CSV files into a single folder.

2. **Remove Duplicate Rows:**  
   Run the `delete_duplicate_rows.py` script to remove duplicates from all CSV files.

3. **Combine CSV Files:**  
   Run the `combine_7_csv.py` script to join the 7 files using PostgreSQL (or another SQL platform).  
   **Specify the following:**  
   - PostgreSQL database connection parameters  
   - Path to the folder containing the 7 CSV files  
   - Path to the export directory  
   - Table name  

4. **Add Hospital Information:**  
   Run the `join_facilities_to_prices.py` script to enrich the combined table with hospital information.  
   **Specify the following:**  
   - Path for logging  
   - Path to the combined CSV table (Step 3 output, `ppo_file`)  
   - Path to the hospital dataset (`taxonomy_file`)  
   - Path to the output file (`final_combined_table`)  

5. **Modify and Finalize Data:**  
   Run the `modify_columns.py` script to remove unnecessary columns and add procedure names.  
   **Specify the following:**  
   - Path to the `final_combined_table` from Step 4 (`main_table_path`)  
   - Path to the procedure names table (`lookup_table_path`)  

---

## **Final Output**
The final output is a `final_processed_table`, ready for use in the application.
