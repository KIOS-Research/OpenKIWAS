import json
import sys

def combine_json_files(json_files):
    combined_data = {}
    for file in json_files:
        with open(file, 'r') as f:
            data = json.load(f)
            combined_data.update(data)

    return combined_data

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python combine_json_files.py <json_file1> <json_file2> ...")
        sys.exit(1)

    json_files = sys.argv[1:]
    combined_data = combine_json_files(json_files)

    with open("combined_data.json", "w") as f:
        json.dump(combined_data, f, indent=4)

    print("Combined JSON output saved to combined_data.json")