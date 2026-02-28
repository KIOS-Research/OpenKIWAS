import csv
import sys

def validate_and_fix_csv(input_file, output_file, expected_columns, delimiter=','):
	"""
	Validate and fix a CSV file by ensuring that each row has the expected number of columns
	:param input_file: Input CSV file
	:param output_file: Output CSV containing the validated data
	:param expected_columns: Expected number of columns in each row
	:param delimiter: Delimiter used in the CSV file
	:return: None
	"""
	with open(input_file, 'r') as infile:
		reader = csv.reader(infile, delimiter=delimiter)
		out = ""
		count = 0
		for line in reader:
			# Remove null cells and shift data to the left
			line = [cell for cell in line if cell]
			if count == 1:
				out += "|||||||||||\n"
			# Count the number of columns
			column_count = len(line)

			if column_count < expected_columns:
				# Add empty values to the right to match the expected column count
				line += [''] * (expected_columns - column_count)
			elif column_count > expected_columns:
				# Remove extra values from the right to match the expected column count
				line = line[:expected_columns]
			out += delimiter.join(line) + "\n"
			count += 1
	out += "|||||||||||\n"
	with open(output_file, 'w', newline='') as outfile:
		outfile.write(out)



if __name__ == "__main__":
	args = sys.argv
	# Check if sufficient arguments are provided
	if len(args) != 4:
		print("Usage: python csv_validator.py <input_csv> <output_csv> <expected_column_count>")
		sys.exit(1)  # Exit the script if arguments are insufficient

	input_csv = args[1]
	output_csv = args[2]
	expected_column_count = int(args[3])
	validate_and_fix_csv(input_csv, output_csv, expected_column_count, delimiter='|')
	print(f"CSV has been validated and saved")
