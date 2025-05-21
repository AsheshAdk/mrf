import gzip
import json
import os

def extract_nr_pr(folder):
    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)
    output_folder = 'output/data.json'
    data_list = []
    
    if not os.path.isdir(folder):
        print(f"{folder} is not a folder!")
        return None

    for file in os.listdir(folder):
        if file.endswith('.gz'):
            try:
                with gzip.open(os.path.join(folder, file), 'rt') as f:
                    data_list.append(json.load(f))
            except:
                pass  

    if data_list:
        with open(output_folder, 'w') as f:
            json.dump(data_list, f)
        print(f"Saved to {output_folder}")
    else:
        print("No data found!")

    return output_folder