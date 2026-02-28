### Program Overview

This program is designed to analyze project data from the CORDIS database, generate prompts for a generative AI model, and process the results. The main components of the program include:

1. **Fetching Data**: The program fetches XML data from the CORDIS database using project IDs. 
2. **Parsing XML**: It parses the XML data to extract relevant information about the projects.
3. **Generating Prompts**: It generates prompts based on the extracted data.  ```xml_prompt_generator.py``` is used to fetch the data, parse it, and generate prompts.
4. **Calling Generative AI**: The program calls a generative AI model to analyze the prompts and generate outputs. ```llm_analysis.py``` is used to generate prompts.
5. **Validating and Saving Results**: It validates the generated CSV files and saves the final results in csv and json formats.

### Dependencies

1. **Python 3.8+**: The program is written in Python and requires Python 3.8 or higher to run.
2. **Python Packages**: The program uses several Python packages, which can be installed using the `requirements.txt` file.
3. **Google Generative AI API Key**: The program requires a Google Generative AI API key to call the generative AI model. You can obtain an API key by navigating to the [Google AI Studio](https://aistudio.google.com/app/apikey).
4. **Input Data**: The program requires an input Excel file containing project IDs. The Excel file should have a column named `Project_ID` containing the project IDs. Also make sure that cross-ref files are present in the same directory as the input Excel file.

### Pre-requisites
1. Find the projectPublications from cordis for the fp7, h2020 and heu projects in exel format. Note That the column title for the fp7 have to be renamed to match the h2020 and heu column titles.
2. Use the `cross-ref.py` to cross-reference papers with project ids you are interested in. Note that project ids have to be in exel file with the column name `Project_ID`.
3. name the output `cross-ref-fp7.csv`, `cross-ref-h2020.csv` and `cross-ref-heu.csv` respectively.

### How to Run

1. **Install Dependencies**: Ensure you have the required Python packages installed. You can install them using the `requirements.txt` file:
```sh 
pip install -r requirements.txt
```

2. **Set Up API Key**: Create a `.env` file in the project directory and add your Google Generative AI API key navigate to the [Google AI Studio](https://aistudio.google.com/app/apikey).
```env
GEN_AI_API_KEY=your_api_key_here
```

3. **Run the Main Script**: Execute the `driver.py` script with the input Excel file containing project IDs:
```sh
python driver.py OpenKIWAS\ Projects.xlsx
```

