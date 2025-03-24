import os
import pandas as pd
import glob

# Define the path to the CSV file
csv_file_path = "/Users/arturtrebski/Documents/Coding_Projects/Lassa_Reports_Scraping/data/documentation/website_raw_data.csv"

# Define the folders to search for PDF files
pdf_folders = [
    "/Users/arturtrebski/Documents/Coding_Projects/Lassa_Reports_Scraping/data/processed/PDFs_Lines_2021",
    "/Users/arturtrebski/Documents/Coding_Projects/Lassa_Reports_Scraping/data/processed/PDFs_Lines_2022",
    "/Users/arturtrebski/Documents/Coding_Projects/Lassa_Reports_Scraping/data/processed/PDFs_Lines_2023",
    "/Users/arturtrebski/Documents/Coding_Projects/Lassa_Reports_Scraping/data/processed/PDFs_Lines_2024",
    "/Users/arturtrebski/Documents/Coding_Projects/Lassa_Reports_Scraping/data/processed/PDFs_Lines_2025"
]

# Load the CSV file
df = pd.read_csv(csv_file_path)

# Add new columns 'Enhanced' and 'Enhanced_name' if they don't exist
if 'Enhanced' not in df.columns:
    df['Enhanced'] = ""
if 'Enhanced_name' not in df.columns:
    df['Enhanced_name'] = ""

# Get all PNG files from the specified folders
png_files = []
for folder in pdf_folders:
    if os.path.exists(folder):
        png_files.extend(glob.glob(os.path.join(folder, "*.png")))

# Create a dictionary to map from original PDF names to PNG files
pdf_to_png = {}

# Process each PNG file name to get the original PDF name
# Format of PNG file: Lines_{pdf.replace('.pdf','')}_page3.png
for png_file in png_files:
    filename = os.path.basename(png_file)
    if filename.startswith("Lines_") and filename.endswith("_page3.png"):
        # Extract the original PDF name by removing "Lines_" and "_page3.png"
        original_name = filename[6:].replace("_page3.png", ".pdf")
        pdf_to_png[original_name] = filename

# Create a separate table of PDF and PNG names (not saving to file)
pdf_png_table = pd.DataFrame({
    'pdf_name': list(pdf_to_png.keys()),
    'png_name': list(pdf_to_png.values())
})

print(f"Found {len(pdf_png_table)} PNG files with corresponding PDF names")

# Update the 'Enhanced' column to 'Y' and 'Enhanced_name' to the PNG filename where there's a matching PDF name
for index, row in df.iterrows():
    if row['new_name'] in pdf_to_png:
        df.at[index, 'Enhanced'] = 'Y'
        df.at[index, 'Enhanced_name'] = pdf_to_png[row['new_name']]
    else:
        df.at[index, 'Enhanced'] = ''
        df.at[index, 'Enhanced_name'] = ''

# Save the updated CSV file
df.to_csv(csv_file_path, index=False)

print(f"CSV file updated successfully. Number of Enhanced=Y entries: {sum(df['Enhanced'] == 'Y')}")