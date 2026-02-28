from bs4 import BeautifulSoup
import requests
import sys
from io import StringIO

from fetch_abstracts import fetch_abstracts
from find_papers import find_papers_by_project_id



def fetch_data_from_url(url):
	"""
	:param url: URL to fetch data from
	:return: response text if successful, None otherwise
	"""
	# Placeholder function to fetch data from a URL
	response = requests.get(url)
	if response.status_code == 200:
		return response.text
	else: # Handle errors
		return None

def parse_xml_and_create_prompt(xml_input_facts, project_id):
	"""
	:param xml_input_facts:  XML input data
	:param project_id: Project ID
	:return: Prompt string
	"""
	def get_text_or_none(soup_txt, tag_name):
		"""
		Extracts text from a BeautifulSoup object based on a tag name.
		:param soup_txt: BeautifulSoup object containing the parsed HTML/XML.
		:param tag_name: Name of the tag to extract text from.
		:return: Text content of the selected element, or None if not found.
		"""
		try:
			return soup_txt.find(tag_name).get_text()
		except AttributeError:
			return None

	def get_text_from_soup(soup_text, selector, attr=None):
		"""
    	Extracts text from a BeautifulSoup object based on a CSS selector.
    	:param soup_text: BeautifulSoup object containing the parsed HTML/XML.
    	:param selector: CSS selector to identify the desired element.
   	 	:param attr: Optional attribute to extract from the selected element.
    	:return: Text content or attribute value of the selected element, or None if not found.
   		"""
		try:
			element = soup_text.select_one(selector)
			return element[attr] if attr else element.get_text(strip=True)
		except AttributeError:
			return None

	soup_fact = BeautifulSoup(xml_input_facts, 'xml')
	xml_input_reports = fetch_data_from_url(f"https://cordis.europa.eu/project/id/{project_id}/reporting")
	soup = BeautifulSoup(xml_input_reports, 'html.parser')
	li_element = soup.find('li', class_='c-article__download-item c-article__download-xml')
	link = li_element.find('a', class_='o-btn o-btn--small c-btn--xml c-link-btn')['href'] if li_element else None

	if not link:
		print("Link not found")
		return

	xml_input_reports = fetch_data_from_url(link)
	soup_report = BeautifulSoup(xml_input_reports, 'xml')

	prompt = StringIO()
	prompt.write("BACKGROUND:\n")
	prompt.write(f"{get_text_or_none(soup_report, 'title')}\n")
	prompt.write(f"Reporting Period: {get_text_or_none(soup_fact, 'startDate')} - {get_text_or_none(soup_fact, 'endDate')}\n")
	prompt.write(f"Summary of the context of the overall project {get_text_or_none(soup_report, 'summary')} \n {get_text_or_none(soup_fact, 'objective')}\n")
	prompt.write("""TASK:
Analyze the text below to identify the different tools developed in the project (focus on tools developed by the projects and not on reports or papers), based only on the titles of the deliverables, reports and published papers.  Put all the tools in a table under those columns delimited with pipes, as follows:
ID|Name|Technology|Type|Data used as input|Produced datasets (openly available)|Demo (video if available)|Paper (if available)|Paper DOI (if available)|Project ID (if available)|Project Acronym (if available)|Service description
Make sure you cross-reference papers and deliverables. Just the create the table, csv optimized with | as a delimiter(DO NOT ADD ANY CODE BLOCKS in the response), make sure that there are ONLY 12 Columns. GIVE ONLY THE PLAIN TABLE IN THE RESPONSE!!!
""")
	prompt.write("DATA:\n")

	try:
		deliverables = find_papers_by_project_id(project_id)
		prompt.write(f"Deliverables (papers):\n")
		for index, deliverable in deliverables.iterrows():
			prompt.write(f"	Title: {deliverable['title']}\n")
			prompt.write(f"	Authors: {deliverable.get('authors', '')}\n")
			prompt.write(f"	Journal Article: {deliverable.get('journalTitle', '')}\n")
			prompt.write(f"	DOI: {deliverable.get('doi', 'No DOI available')}\n")
			prompt.write(f"	Abstract: {deliverable.get('abstract', 'No abstract available')}\n\n")
	except:
		deliverables = soup_fact.find_all('result', type='relatedResult')
		prompt.write(f"Deliverables: \n")
		for deliverable in deliverables:
			prompt.write(f"	Title: {get_text_or_none(deliverable, 'title')}\n")
			prompt.write(f"	Authors: {get_text_or_none(deliverable, 'authors')}\n")
			prompt.write(f"	Journal Article: {get_text_or_none(deliverable, 'journalTitle')}\n")
			prompt.write(f"	Publisher: {get_text_or_none(deliverable, 'publisher')}\n")
			prompt.write(f"	DOI: {get_text_or_none(deliverable, 'doi')}\n")
			if deliverable.find('doi'):
				abstract = fetch_abstracts(deliverable.find('doi').get_text())
				prompt.write(f"Abstract: {abstract}\n")
			prompt.write("\n")
	prompt.write(f"Project ID: {project_id}\n")
	prompt.write(f"Project Acronym: {get_text_or_none(soup_fact, 'acronym')}\n")
	prompt.write(f"Grand doi: {get_text_or_none(soup_fact, 'grantDoi')}\n")
	prompt.write(f"EC Signature Date: {get_text_or_none(soup_fact, 'ecSignatureDate')}\n")
	prompt.write(f"Start Date: {get_text_or_none(soup_fact, 'startDate')}\n")
	prompt.write(f"End Date: {get_text_or_none(soup_fact, 'endDate')}\n")
	prompt.write(f"Funded under: {get_text_from_soup(soup, 'div.c-project-info__fund li')}\n")
	prompt.write(f"{get_text_from_soup(soup, 'div.c-project-info__overall').replace('\t', '')}\n")
	prompt.write(f"{get_text_from_soup(soup, 'div.c-project-info__eu').replace('\t', '')}\n")
	prompt.write(f"\nProject Coordinator: {get_text_from_soup(soup, 'p.coordinated.coordinated-name')}\n")
	return prompt.getvalue()


def xml_prompt_gen(project_id):
	"""
	:param project_id:  Project ID
	:return:  Prompt string
	"""
	url_fact = f"https://cordis.europa.eu/project/id/{project_id}?format=xml"
	xml_data_fact = fetch_data_from_url(url_fact)
	if xml_data_fact is None:
		print("Error fetching XML data")
		sys.exit(1)
	prompt = parse_xml_and_create_prompt(xml_data_fact, project_id)
	prompt += "\nGIVE ONLY THE PLAIN TABLE IN THE RESPONSE!!!\nAnd also make sure that there are ONLY 12 Columns and insure proper alignment of the columns\n"
	return prompt

if __name__ == "__main__":
	arguments = sys.argv
	# Check if sufficient arguments are
	# provided
	if len(arguments) < 2:
		print("Usage: python xml_prompt_gen.py <project_id>")
		sys.exit(1)  # Exit the script if arguments are insufficient

	# Get the project ID from the command line arguments
	project_id = arguments[1]
	xml_prompt_gen(project_id)

