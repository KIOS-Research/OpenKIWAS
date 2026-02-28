import pandas as pd
import sys

# Access command-line arguments
arguments = sys.argv

# Default output file
output_file = 'cross_referenced_output.csv'

# Check if the correct number of arguments is provided
if len(arguments) < 3:
	print("Usage: python cross-ref.py <input_file1> <input_file2> [output_file]")
	sys.exit(1)
elif len(arguments) == 4:
	output_file = arguments[3]

# Get input file paths
first_file = arguments[1]
second_file = arguments[2]

try:
	# Load the first Excel file
	print(f"Reading first file: {first_file}")
	df_first = pd.read_excel(first_file)

	# Load the second Excel file
	print(f"Reading second file: {second_file}")
	df_second = pd.read_excel(second_file)

	# Ensure no missing or non-numeric values in the relevant columns
	print("Cleaning and validating columns...")
	df_first = df_first[pd.to_numeric(df_first['Project_ID'], errors='coerce').notna()]
	df_second = df_second[pd.to_numeric(df_second['projectID'], errors='coerce').notna()]

	# Convert columns to integers
	df_first['Project_ID'] = df_first['Project_ID'].astype(int)
	df_second['projectID'] = df_second['projectID'].astype(int)

	# Cross-referencing: Match 'Project_ID' from the first file with 'projectID' from the second
	print("Performing cross-referencing...")
	matched_df = df_second[df_second['projectID'].isin(df_first['Project_ID'])]

	# Save the matched rows to a new CSV file
	print(f"Saving results to {output_file}...")
	matched_df.to_csv(output_file, sep=';', index=False)

	print(f"Cross-referenced rows successfully saved to {output_file}")

except FileNotFoundError as e:
	print(f"Error: {e}. Please ensure the file paths are correct.")
except KeyError as e:
	print(f"Error: Column {e} not found in one of the files. Please check column names.")
except Exception as e:
	print(f"An unexpected error occurred: {e}")
