import json
import requests
import threading
import time
from bs4 import BeautifulSoup

def extract_heading(xml_url):
    try:
        response = requests.get(xml_url)
        response.raise_for_status()
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'xml')
            ueberschrift_tag = soup.find('ueberschrift', typ='para')
            if ueberschrift_tag:
                return ueberschrift_tag.text.strip()
            else:
                return ""
        else:
            print(f"Failed to fetch XML content from URL: {xml_url}")
            return ""
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch XML content from URL: {xml_url}, Error: {e}")
        return ""

    
# Function to extract data for a given paragraph
def extract_data(law_id, start, end, fassung):
    url = f'https://data.bka.gv.at/ris/api/v2.6/Bundesrecht?Applikation=BrKons&Gesetzesnummer={law_id}&Abschnitt.Typ=Paragraph&Abschnitt.Von={start}&Abschnitt.Bis={end}&Fassung.FassungVom={fassung}'
    try:
        print(f"Fetching data for Law {law_id}, Paragraph {start}...")
        response = requests.get(url)
        response.raise_for_status()
        if response.status_code == 200:
            data = response.json()
            extracted_data = []
            for doc in data.get('OgdSearchResult', {}).get('OgdDocumentResults', {}).get('OgdDocumentReference', []):
                try:
                    metadata = doc.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {})
                    technisch = doc.get('Data', {}).get('Metadaten', {}).get('Technisch', {})
                    bgbl_kons = doc.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {}).get('BrKons', {})
                    if bgbl_kons.get('Uebergangsrecht'):
                        continue  # Skip this document and continue with the next one
                except:
                    data_new = data.get('OgdSearchResult', {}).get('OgdDocumentResults', {}).get('OgdDocumentReference', {})
                    technisch = data_new.get('Data', {}).get('Metadaten', {}).get('Technisch', {})
                    metadata = data_new.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {})
                    bgbl_kons = data_new.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {}).get('BrKons', {})
                    if bgbl_kons.get('Uebergangsrecht'):
                        continue
                paragraph_id = technisch.get('ID','Keine Paragraphen-ID gefunden')
                kurztitel = metadata.get('Kurztitel', 'Kein Kurztitel gefunden')
                typ = bgbl_kons.get('Dokumenttyp','Keinen Dokumententyp gefunden')
                buchstabe = bgbl_kons.get('Paragraphbuchstabe','')
                paragraph_nummer = bgbl_kons.get('Paragraphnummer', 'Keine Nummer für Paragraphen gefunden')
                paragraph_nummer = paragraph_nummer+buchstabe
                link = f'https://www.ris.bka.gv.at/Dokumente/Bundesnormen/{paragraph_id}/{paragraph_id}.xml'
                # Extract heading from XML content
                heading = extract_heading(link)
                
                extracted_data.append({'Abfragenummer': start, 'Gesetzesnummer': law_id, 'Kurztitel': kurztitel, 'Paragraph': paragraph_nummer, 'Typ': typ, 'Paragraph-ID': paragraph_id, 'Link': link, 'Überschrift': heading})
            return extracted_data
        else:
            print(f"Failed to fetch data for Law {law_id}, Paragraphs {start} to {end}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for Law {law_id}, Paragraphs {start} to {end}: {e}")
        return None

# Function to check if a paragraph is empty
def is_paragraph_empty(law_id, paragraph_id, fassung):
    url = f'https://data.bka.gv.at/ris/api/v2.6/Bundesrecht?Applikation=BrKons&Gesetzesnummer={law_id}&Abschnitt.Typ=Paragraph&Abschnitt.Von={paragraph_id}&Abschnitt.Bis={paragraph_id}&Fassung.FassungVom={fassung}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        if response.status_code == 200:
            data = response.json()
            if not data.get('OgdSearchResult', {}).get('OgdDocumentResults', {}).get('OgdDocumentReference', []):
                return True
            else:
                return False
        else:
            print(f"Failed to fetch data for Law {law_id}, Paragraph {paragraph_id}: {response.status_code}")
            return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for Law {law_id}, Paragraph {paragraph_id}: {e}")
        return True

# Function to extract data for a given range of paragraphs
def extract_data_range(law_id, start_par, end_par, thread_index, results):
    for paragraph_id in range(start_par, end_par + 1):
        data = extract_data(law_id, paragraph_id, paragraph_id, '2024-01-01')
        if data:
            results[thread_index].extend(data)
        time.sleep(0.5)  # Adding a short delay to avoid overwhelming the API

# User-specified law ID
law_id = 10004570  # Replace with the appropriate law ID

# Determine the start paragraph ID
start_par = 1
while is_paragraph_empty(law_id, start_par, '2024-01-01'):
    start_par += 1

# Determine the end paragraph ID
empty_count = 0
end_par = start_par
while empty_count < 10:  # Stop if 100 consecutive empty paragraphs are found
    if is_paragraph_empty(law_id, end_par, '2024-01-01'):
        empty_count += 1
    else:
        empty_count = 0
    end_par += 10

# Initialize results list for each thread
num_threads = 100
results = [[] for _ in range(num_threads)]

# Determine the number of paragraphs each thread should handle
num_paragraphs_per_thread = (end_par - start_par + 1) // num_threads

# Create and start threads
threads = []
for i in range(num_threads):
    thread_start_par = start_par + i * num_paragraphs_per_thread
    thread_end_par = min(start_par + (i + 1) * num_paragraphs_per_thread - 1, end_par)
    thread = threading.Thread(target=extract_data_range, args=(law_id, thread_start_par, thread_end_par, i, results))
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
output_file = f'{law_id}_liste_paragraphen.json'
with open(output_file, 'w', encoding='utf-8') as f:  # Ensure UTF-8 encoding when writing to the file
    json.dump(extracted_data, f, indent=4, ensure_ascii=False)  # Ensure non-ASCII characters are not escaped
