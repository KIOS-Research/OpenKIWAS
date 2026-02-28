import os

import google.generativeai as genai
import time
from dotenv import load_dotenv
import sys

from xml_prompt_gen import xml_prompt_gen
from csv_validator import validate_and_fix_csv


def init():
	"""
	Initialize the Generative AI model
	:return: None
	"""
	# Getting API key to use the google generative ai model
	# Make sure to create the .env file and add the GEN_AI_API_kEY which you can get from here: https://aistudio.google.com/app/apikey
	# Load API key from .env file
	load_dotenv()
	google_api_key = os.getenv("GEN_AI_API_KEY")
	if not google_api_key:
		raise ValueError("API key not found! Make sure GEN_AI_API_KEY is set in the .env file.")

	# Configure the Generative AI model
	genai.configure(api_key=google_api_key)

# Function to call the Google Generative AI API for a single prompt using the GenerativeModel class.
def call_genai(gen_prompt):
	wait_time = 1  # Start with a short wait time
	while True:
		try:
			model = genai.GenerativeModel('gemini-1.5-flash')  # Specify your model here
			response = model.generate_content(gen_prompt)
			return response.text.strip()
		except Exception as e:
			#print(f"An error occurred: {e}")
			time.sleep(wait_time)
			wait_time *= 2  # Exponential backoff for retrying

def llm_analysis(project_id, output_file):
	"""
	Generate a CSV file using the Generative AI model
	:param project_id:  Project ID
	:param output_file:  Output file to save the generated CSV
	:return:
	"""
	# Load the API key
	init()
	# Generate the prompt

	prompt = xml_prompt_gen(project_id)
	if not prompt:
		print("Prompt generation failed. Exiting...")
	# Call the Generative AI model with the generated prompt
	result = call_genai(prompt)
	open(output_file, "w").write(result)
	# Validate the generated CSV file
	validate_and_fix_csv(output_file, output_file, 12, delimiter='|')
	return result

if __name__ == "__main__":
	args = sys.argv
	# Check if sufficient arguments are provided
	if len(args) != 3:
		print("Usage: python llm_analysis.py <output_file> <project_id>")
		sys.exit(1)  # Exit the script if arguments are insufficient
	output_file = args[1]
	project_id = int(args[2])
	print(llm_analysis(project_id, output_file))
	sys.exit(0)  # Exit the script with success status
