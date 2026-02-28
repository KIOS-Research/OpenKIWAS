import glob
import os
import sys
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from llm_analysis import llm_analysis

def save_as_json(df, output_file):
    df = df.iloc[:, :12]
    df = df.dropna(how='all')
    df = df.dropna(subset=['Project ID (if available)', 'ID'])
    df = df[pd.to_numeric(df['Project ID (if available)'], errors='coerce').notna()]
    df = df[pd.to_numeric(df['ID'], errors='coerce').notna()]
    df['Project ID (if available)'] = df['Project ID (if available)'].astype(int)
    df['ID'] = df['ID'].astype(int)
    df['Project ID (if available)'] = df['Project ID (if available)'].astype(str)

    result = {}
    # Group by project id and create a dictionary of children for each project
    for (project_id,project_acronym), group in df.groupby(["Project ID (if available)","Project Acronym (if available)"]):
        children = []
        for _, row in group.iterrows():
            child = row.drop(["ID", "Project ID (if available)","Project Acronym (if available)"]).fillna("").to_dict()
            child["ID"] = row["ID"]
            children.append(child)
        if project_id not in result:
            result[project_id] = {}
        result[project_id][project_acronym] = children
    json_result = json.dumps(result, indent=4)
    with open(output_file, "w") as f:
        f.write(json_result)
    print(f"JSON output saved to {output_file}")


#%%
def concatenate_output (output_file="output/llm_output.xlsx", json_output_file="output/llm_output.json"):
    # Concatenate all the output files into a single file
    all_files = glob.glob("output/*.csv")
    # add header to the dataframe ID,Name,Technology,Type,Data used as input,Produced datasets (openly available),Demo (video if available),Paper (if available),Paper DOI (if available),Project ID (if available),Service description
    df = pd.DataFrame()
    # Specify the data types for the columns
    # ignore the frist two rows of  every file and add one row containing the project id of the file
    for i in tqdm(range(len(all_files))):
        # add row containing the project id in the first column
        df_con = pd.read_csv(all_files[i], delimiter='|', encoding='utf-8', on_bad_lines='skip', index_col=False, engine='python')
        df_con.columns = ['ID', 'Name', 'Technology', 'Type', 'Data used as input', 'Produced datasets (openly available)', 'Demo (video if available)', 'Paper (if available)', 'Paper DOI (if available)', 'Project ID (if available)', 'Project Acronym (if available)', 'Service description']
        df = pd.concat([df, df_con], ignore_index=True)
    df.to_excel(output_file, index=False)
    print(f"All output files have been concatenated into {output_file}")
    save_as_json(df, json_output_file)

#%%
def main():
    try:
        os.mkdir("output")
    except FileExistsError:
        pass
    except Exception as e:
        raise e
    # clear the output directory
    files = glob.glob('output/*.csv')
    for f in files:
        os.remove(f)

    args = sys.argv
    # Check if sufficient arguments are provided
    if len(args) != 2:
        print("Usage: python llm_analysis.py <input_file> ")
        sys.exit(1)  # Exit the script if arguments are insufficient
    input_file = args[1]
    df = pd.read_excel(input_file, engine='openpyxl')
    project_ids = df[pd.to_numeric(df['Project_ID'], errors='coerce').notna()]
    project_ids = project_ids['Project_ID'].astype(int)
    progress = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(llm_analysis, project_id, f"output/{project_id}.csv"): project_id for project_id in project_ids}
        for future in tqdm(as_completed(futures), total=len(futures)):
            progress += 1
#%%
    concatenate_output()
#%%
if __name__ == "__main__":
    main()  # Call the main function

