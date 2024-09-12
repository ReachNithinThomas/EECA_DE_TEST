import os
import requests
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def download_file(url, directory_path):
    file_name = url.split('/')[-1]
    destination_path = os.path.join(directory_path, file_name)

    response = requests.get(url)
    if response.status_code == 200:
        print(f"Downloading '{file_name}'...")
        with open(destination_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        print(f"Downloaded '{file_name}' to '{destination_path}'")
        
        if zipfile.is_zipfile(destination_path):
            with zipfile.ZipFile(destination_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    if file_info.filename.endswith('.csv') and not file_info.filename.startswith('__MACOSX/'):
                        print(f"Extracting {file_info.filename}...")
                        zip_ref.extract(file_info, directory_path)
                        print(f"Extracted '{file_info.filename}' to '{directory_path}'")
                    # Delete the original ZIP file after extraction
        try:
            os.remove(destination_path)
            print(f"Deleted original ZIP file '{destination_path}'")
        except Exception as e:
            print(f"Failed to delete original ZIP file '{destination_path}': {e}")
    else:
        print(f"Failed to download: {file_name}")
    

def main():
    directory_path = "downloads"
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created.")
    else:
        print(f"Directory '{directory_path}' already exists.")
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(download_file, url, directory_path) for url in download_uris]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred: {e}")
    pass

if __name__ == "__main__":
    main()