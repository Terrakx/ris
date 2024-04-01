import json
import requests
import re
import threading
import time

# Function to extract the year from an ELI string
def eli_regex(x):
    pattern = r'/(\d+)/'
    regex_pattern = re.search(pattern, x)
    regex_year = regex_pattern.group(1) if regex_pattern else 'Unknown Year'
    return regex_year

# Function to extract data for a given ID range
def extract_data_range(start_id, end_id, thread_index, results):
    for id in range(start_id, end_id + 1):
        data = extract_data(id)
        if data:
            results[thread_index].append(data)
        time.sleep(0.5)  # Adding a short delay to avoid overwhelming the API

# Function to extract data for a specific ID
def extract_data(id):
    url = f'https://data.bka.gv.at/ris/api/v2.6/Bundesrecht?Applikation=BrKons&Gesetzesnummer={id}'
    try:
        print(f"Fetching data for ID {id}...")
        response = requests.get(url)
        response.raise_for_status()
        if response.status_code == 200:
            data = response.json()
            extracted_data = []
            for doc in data.get('OgdSearchResult', {}).get('OgdDocumentResults', {}).get('OgdDocumentReference', []):
                try:
                    metadata = doc.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {})
                    bgbl_kons = doc.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {}).get('BrKons', {})
                except:
                    data_new = data.get('OgdSearchResult', {}).get('OgdDocumentResults', {}).get('OgdDocumentReference', {})
                    metadata = data_new.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {})
                    bgbl_kons = data_new.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {}).get('BrKons', {})
                kurztitel = metadata.get('Kurztitel', 'Kein Kurztitel gefunden')
                titel = metadata.get('Titel', 'Kein vollständiger Titel gefunden')
                eli_year_match = eli_regex(metadata.get('Eli'))
                abkuerzung = bgbl_kons.get('Abkuerzung', 'Keine Abkürzung gefunden')
                typ = bgbl_kons.get('Dokumenttyp','Keinen Dokumententyp gefunden')
                gesetzesnummer = bgbl_kons.get('Gesetzesnummer', 'Keine Gesetzesnummer gefunden')
                extracted_data = {'ID': id, 'Kurztitel': kurztitel, 'Titel': titel, 'Eli year': eli_year_match, 'Abkuerzung': abkuerzung, 'Gesetzesnummer': gesetzesnummer}
                #print(json.dumps(extracted_data, ensure_ascii=False))  # Print the extracted data without escaping non-ASCII characters
                return extracted_data
            return extracted_data
        else:
            print(f"Failed to fetch data for ID {id}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for ID {id}: {e}")
        return None

# User-specified start and end IDs
start_id = 10000001
end_id = 10013000
num_threads = 100

# Determine the number of cases each thread should handle
num_cases_per_thread = (end_id - start_id + 1) // num_threads

# Initialize results list for each thread
results = [[] for _ in range(num_threads)]

# Create and start threads
threads = []
for i in range(num_threads):
    thread_start_id = start_id + i * num_cases_per_thread
    thread_end_id = min(start_id + (i + 1) * num_cases_per_thread - 1, end_id)
    thread = threading.Thread(target=extract_data_range, args=(thread_start_id, thread_end_id, i, results))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Combine results from all threads
extracted_data = []
for thread_result in results:
    extracted_data.extend(thread_result)

# Save extracted data to a JSON file
output_file = 'liste_gesetze.json'
with open(output_file, 'w', encoding='utf-8') as f:  # Ensure UTF-8 encoding when writing to the file
    json.dump(extracted_data, f, indent=4, ensure_ascii=False)  # Ensure non-ASCII characters are not escaped
