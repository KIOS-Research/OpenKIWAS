import pandas as pd
import sys

def find_papers_by_project_id(project_ids):
	"""
	Find papers associated with the given project ID
	:param project_ids: Project ID to search for, output the papers associated with the project ID in the standard output
	:return: DataFrame containing the papers associated with the project ID
	"""

	# Read the csv files

	df_fp7 = pd.read_excel('projectPublicationsfp7_ab.xlsx')
	df_h2020 = pd.read_excel('projectPublicationsh2020_ab.xlsx')
	df_heu = pd.read_excel('projectPublicationsheu_ab.xlsx')
	columns = ['title', 'doi', 'projectID', 'authors', 'journalTitle', 'abstract']
	# Filter the DataFrame to find papers associated with the given project ID
	matched_papers = df_fp7[df_fp7['projectID'] == project_ids]
	if matched_papers.empty:
		matched_papers = df_h2020[df_h2020['projectID'].astype(str).str.contains(str(project_ids))]
	if matched_papers.empty:
		matched_papers = df_heu[df_heu['projectID'].astype(str).str.contains(str(project_ids))]
	# Check if any papers were found
	if matched_papers.empty:
		print(f"No papers found for project ID: {project_ids}")
		return None
	else:
		# replace empty cell in abstract with 'No abstract available'
		try:
			matched_papers['abstract'].fillna('No abstract available')
		except KeyError:
			pass
		return matched_papers[columns]

if __name__ == "__main__":
	# Check if sufficient arguments are provided
	if len(sys.argv) != 2:
		print("Usage: python find_papers.py <project_id>")
		sys.exit(1)
	project_id = sys.argv[1]
	find_papers_by_project_id(int(project_id))



