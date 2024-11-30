'''
Reduce el tamaño del dataset
'''

import pandas as pd
import sys
from utils.env_loader import EnvLoader

env_loader = EnvLoader()
books_path = env_loader.books_path
ratings_path = env_loader.ratings_path

def read_books():
    return pd.read_csv(books_path)

def write_books(books_df):
    books_df.to_csv(books_path.replace('.csv', '_reduced.csv'), index=False)

def read_ratings():
    return pd.read_csv(ratings_path)

def write_ratings(ratings_df):
    ratings_df.to_csv(ratings_path.replace('.csv', '_reduced.csv'), index=False)

def reduce_dataset(books: int):
    '''
    Reduces the dataset to the first 'books' books
    Produces the new files named with _reduced
    '''
    
    # Get first books
    books_df = read_books()
    books_df = books_df.sample(n=books, random_state=1)
    write_books(books_df)
    print("Libros seleccionados y CSV creado.")
    
    # Get ratings for those books
    ratings_df = read_ratings()
    ratings_df = ratings_df[ratings_df['Title'].isin(books_df['Title'])]
    write_ratings(ratings_df)
    print("Reseñas seleccionadas y CSV creado.")
    print(f'Dataset reduced to {books} books')
    
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python reduce-dataset.py <books>')
        sys.exit(1)
    reduce_dataset(int(sys.argv[1]))