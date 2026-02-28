import sys

import requests
from bs4 import BeautifulSoup

class CrossRefAPI:
	"""
	Class to fetch abstracts from CrossRef API
	"""
	def __init__(self):
		"""
		Initialize the session
		"""
		self.session = requests.Session()
		self.session.headers.update({'User-Agent': 'Company Name (email@example.com)'})

	def fetch_abstract(self, doi):
		"""
		Fetch abstract from CrossRef API
		:param doi:  DOI of the paper
		:return:  Abstract of the paper
		"""
		url = f"https://api.crossref.org/works/{doi}"
		try:
			response = self.session.get(url)
			if response.status_code == 200:
				data = response.json()
				abstract = data['message'].get('abstract', 'No abstract available')
				# Use BeautifulSoup to remove HTML tags
				soup = BeautifulSoup(abstract, 'html.parser')
				clean_abstract = soup.get_text()
				if clean_abstract is None:
					return None
				clean_abstract.replace('\n', ' ').replace('\r', ' ')

				return clean_abstract

			else:
				return None
		except requests.exceptions.RequestException:
			return None
		except Exception as e:
			return None

def fetch_abstracts(doi):
	"""
	Main function to fetch abstracts from CrossRef API
	:return:
	"""

	api = CrossRefAPI()
	abstract = api.fetch_abstract(doi)
	return abstract

if __name__ == '__main__':
	args = sys.argv
	if len(args) != 2:
		print("Usage: python fetch_abstracts.py <doi>")
		sys.exit(1)
	doi = args[1]
	fetch_abstracts(doi)
