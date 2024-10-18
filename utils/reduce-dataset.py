'''
Reduce el tamaño del dataset
'''

import pandas as pd
import os
import sys
import dotenv

dotenv.load_dotenv()
books_path = os.getenv('ALL_BOOKS_PATH')
ratings_path = os.getenv('ALL_RATINGS_PATH')

def reduce_dataset(books: int):
    '''
    Reduces the dataset to the first 'books' books
    Produces the new files named with _reduced
    '''
    
    # Get first books
    books_df = pd.read_csv(books_path)
    books_df = books_df.sample(n=books, random_state=1)
    books_df.to_csv(books_path.replace('.csv', '_reduced.csv'), index=False)
    print("Libros seleccionados y CSV creado.")
    
    # Get ratings for those books
    ratings_df = pd.read_csv(ratings_path)
    ratings_df = ratings_df[ratings_df['Title'].isin(books_df['Title'])]
    ratings_df.to_csv(ratings_path.replace('.csv', '_reduced.csv'), index=False)
    print("Reseñas seleccionadas y CSV creado.")
    print(f'Dataset reduced to {books} books')
    
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python reduce-dataset.py <books>')
        sys.exit(1)
    reduce_dataset(int(sys.argv[1]))