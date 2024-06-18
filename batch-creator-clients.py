import pandas as pd
import os
import numpy as np

file_path = './docker-compose-client.yaml'

def extract_client_ids(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    client_ids = []
    is_commented = False

    for line in lines:
        if line.strip().startswith('#'):
            is_commented = True
        elif line.strip() == '':
            is_commented = False

        if 'CLIENT_ID' in line and not is_commented:
            client_id = line.split('=')[1].strip()
            client_ids.append(client_id)

        if is_commented and not line.strip().startswith('#'):
            is_commented = False

    return client_ids

# Obtener los CLIENT_ID no comentados
client_ids = extract_client_ids(file_path)
output_dirs = [f'./data/{client_id}' for client_id in client_ids]

books_df = pd.read_csv('./data/books_data.csv')
ratings_df = pd.read_csv('./data/Books_rating.csv')
merged_df = pd.merge(books_df, ratings_df, on='Title', how='inner')
num_splits = len(output_dirs)
split_size = len(merged_df) // num_splits
splits = np.array_split(merged_df, num_splits)

for i, split in enumerate(splits):
    if not os.path.exists(output_dirs[i]):
        os.makedirs(output_dirs[i])
    
    split_books = split[['Title', 'description', 'authors', 'image', 'previewLink', 
                         'publisher', 'publishedDate', 'infoLink', 'categories', 'ratingsCount']].drop_duplicates()
    split_ratings = split[['Id', 'Title', 'Price', 'User_id', 'profileName', 
                           'review/helpfulness', 'review/score', 'review/time', 
                           'review/summary', 'review/text']].drop_duplicates()
    
    split_books.to_csv(os.path.join(output_dirs[i], 'books_data.csv'), index=False)
    split_ratings.to_csv(os.path.join(output_dirs[i], 'Books_rating.csv'), index=False)
