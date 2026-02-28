import pandas as pd
import json
import sys
import os


def excel_to_json(excel_file):
    """
    Convert an Excel file to JSON
    :param excel_file: Excel file to convert
    :return: None
    """
    # Read the Excel file
    df = pd.read_excel(excel_file)
    # drop any null rows
    df = df.dropna(how='all')
    # replace any null values with empty string
    df = df.fillna('')
    # Get the file name without extension
    file_name = os.path.splitext(os.path.basename(excel_file))[0]


    # Convert the DataFrame to a list of dictionaries
    data = df.to_dict(orient='records')

    # Create the final JSON structure
    json_data = {file_name: data}

    # Convert to JSON string
    json_str = json.dumps(json_data, indent=4)
    with open(f"{file_name}.json", "w") as f:
        f.write(json_str)

    print(f"JSON output saved to {file_name}.json")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python exel_to_json.py <excel_file>")
        sys.exit(1)

    excel_file = sys.argv[1]
    excel_to_json(excel_file)