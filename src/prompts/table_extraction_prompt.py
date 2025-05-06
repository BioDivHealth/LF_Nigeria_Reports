"""
Table extraction prompt for Lassa fever reports.

This module contains the prompt template used by the Gemini AI model
to extract tabular data from Lassa fever report images.
"""

# Define the prompt with instructions to extract JSON formatted output.
TABLE_EXTRACTION_PROMPT = """
The provided image contains a table with weekly Lassa Fever case data across States in Nigeria. Your task is to extract the data from the table.
The table has the following columns in this exact left-to-right order:
1. States
2. Suspected
3. Confirmed
4. Trend (ignore this column - it is not needed)
5. Probable
6. HCW*
7. Deaths (Confirmed Cases)

Extract the values located under each column headers (States, Suspected, Confirmed, Probable, HCW*, Deaths (Confirmed Cases)) and return the results in JSON format. Do not hallucinate any values if a cell is empty. Process the table row by row.
Ignore the "Trend" column.
Return a JSON list of objects, where each object corresponds to one row of the table.

Each object must have the following keys (exactly in this order):
"States", "Suspected", "Confirmed", "Probable", "HCW*", "Deaths (Confirmed Cases)".

**Important Validation Rules To Avoid Hallucination:**
- Ensure all extracted numbers are non-negative integers.
- Only include numerical values which you see in the image, never create fake data. Include the numbers exactly as you see them in the image, it is very important to maintain accuracy.
- Treat columns as separate, work row by row and column by column.
- Numbers within column are right-aligned, make sure you correctly identify which column does a number belong to. You must not confuse the columns. 

"States" corresponds to the states of Nigeria: Ondo, Edo, Bauchi, Taraba, Benue, Ebonyi, Kogi, Kaduna, Plateau, Enugu, Cross River, Rivers, Delta, Nasarawa, Anambra, Gombe, Niger, Imo, Jigawa, Bayelsa, Adamawa, Fct, Katsina, Kano, Oyo, Lagos, Ogun, Yobe, Sokoto, Kebbi, Zamfara, Akwa Ibom, Ekiti, Kwara, Borno, Osun, Abia. These are the correct names, sometimes there may be a typo in the image.
You should include ONLY the names of the States that you see in the image.
You can only use these names of states, but order may often differ. Not all states have to be included in an image. You need to write the names of States in the order in which they appear in the image you see.

Include one object per State you see in the image, and the last object should correspond to the "Total" row.
Ensure that all keys are present in every object, even if some values are blank.
Output the JSON in valid format.
"""
