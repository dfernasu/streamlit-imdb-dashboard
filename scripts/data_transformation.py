# ///////////////////////////////////////////////////////////////////////
#
#                           DATA TRANFORMATION
#   This script is executed apart from the app, and it is used to transform
#   the original dataset (datasets\imdb_dataset.csv) to create the rest of
#   .csv files.
# ///////////////////////////////////////////////////////////////////////

from dotenv import load_dotenv
import pandas as pd
import re, os

load_dotenv()

# -----------------------------------------------------------------------
#                                GLOBAL_PARAMETERS
# -----------------------------------------------------------------------

PROYECT_FOLDER = os.getenv("PROYECT_FOLDER")
DATASETS_FOLDER = f"{PROYECT_FOLDER}/datasets/"

# -----------------------------------------------------------------------
#                             FUNCTIONS
# -----------------------------------------------------------------------

# Function to separate into a list the actors in a row  
def separate_elements(elements: str):
    return re.split(', |,', str(elements))

# Function that returns the category LOW, MEDIUM or HIGH for a given revenue
def categorize_column(value, min_value, max_value):
    segment = (max_value - min_value) / 3

    if(value < segment):
        return 'LOW'
    elif(value < segment * 2):
        return 'MEDIUM'
    else:
        return 'HIGH'

# Function that returns a category for critic scores between 0 and 100
def categorize_score(score):
    categories = {
        range(95, 101): 'OVERWHELMING_POSITIVE',
        range(80, 95): 'MOSTLY_POSITIVE',
        range(65, 80): 'POSITIVE',
        range(50, 65): 'MIXED',
        range(30, 50): 'NEGATIVE',
        range(6, 30): 'MOSTLY_NEGATIVE',
        range(0, 6): 'OVERWHELMING_NEGATIVE'
    }

    for score_range, category in categories.items():
        if score in score_range:
            return category

# Function to save all the actors of the dataset into a csv
#   with 2 columns: id and name (alphabetical order)
def save_actors_dim(actors_column: pd.Series):
    actor_list = []

    for actors in actors_column:
        #Splits the actors in the str based on ', ' and ','
        actors_splitted = separate_elements(actors)
        
        for actor in actors_splitted:
            actor_list.append(actor.strip())

    actor_list = list(set(actor_list))
    actor_list.sort()
    actors_df = pd.DataFrame(actor_list, columns=['ACTOR_NAME'])

    return actors_df

# Function to save all the genres of the dataset into a csv
#   with 2 columns: id and name (alphabetical order)
def save_genres_dim(genres_column: pd.Series):
    genre_list = []

    for genres in genres_column:
        #Splits the genres in the str based on ', ' and ','
        genres_splitted = separate_elements(genres)
        
        for genre in genres_splitted:
            genre_list.append(genre.strip().upper())

    genre_list = list(set(genre_list))
    genre_list.sort()
    genres_df = pd.DataFrame(genre_list, columns=['GENRE_NAME'])

    return genres_df

# Function to save all the years of the dataset into a csv
#   with 2 columns: id and year (numerical order)
def save_years_dim(years_column: pd.Series):
    
    years_list = list(set(years_column.to_list()))
    years_list.sort()
    years_df = pd.DataFrame(years_list, columns=['YEAR'])

    return years_df

# Function to save all the directors of the dataset into a csv
#   with 2 columns: id and director_name (alphabetical order)
def save_directors_dim(director_column: pd.Series):
    
    director_list = list(set(director_column.to_list()))
    director_list.sort()
    director_list = list(map(str.strip, director_list))
    director_df = pd.DataFrame(director_list, columns=['DIRECTOR_NAME'])

    return director_df

# Function to transform and save/load data into dimensions
def get_dims(imdb: pd.DataFrame, load_from_disk=False):
    
    local_path = f'{DATASETS_FOLDER}dim_'
    d = ','
    h = 0
    extension = '.csv'
    
    if(not load_from_disk):
        genres_df = save_genres_dim(imdb['GENRE'])
        actors_df = save_actors_dim(imdb['ACTORS'])
        years_df = save_years_dim(imdb['YEAR'])
        directors_df = save_directors_dim(imdb['DIRECTOR'])

        genres_df.to_csv(sep=d, path_or_buf=(local_path + 'genres' + extension), index_label=['GENRE_ID'])
        actors_df.to_csv(sep=d, path_or_buf=(local_path + 'actors' + extension), index_label=['ACTOR_ID'])
        years_df.to_csv(sep=d, path_or_buf=(local_path + 'years' + extension), index_label=['YEAR_ID'])
        directors_df.to_csv(sep=d, path_or_buf=(local_path + 'directors' + extension), index_label=['DIRECTOR_ID'])
    
    genres_df = pd.read_csv(local_path + 'genres' + extension, delimiter=d, header=h)
    actors_df = pd.read_csv(local_path + 'actors' + extension, delimiter=d, header=h)
    years_df = pd.read_csv(local_path + 'years' + extension, delimiter=d, header=h)
    directors_df = pd.read_csv(local_path + 'directors' + extension, delimiter=d, header=h)
        
    print(f'Num of genres: {len(genres_df)}')
    print(f'Num of actors: {len(actors_df)}')
    print(f'Num of years: {len(years_df)}')
    print(f'Num of directors: {len(directors_df)}')

    return genres_df, actors_df, years_df, directors_df

# Function transform and save/load data into bridge tables for the actors and genres
def get_bridge_tables(imdb: pd.DataFrame, genres_df: pd.DataFrame, actors_df: pd.DataFrame, load_from_disk=False):
    
    local_path = f'{DATASETS_FOLDER}bridge_'
    d = ','
    h = 0
    extension = '.csv'

    if(not load_from_disk):
        film_ranks_genres_list = []
        film_ranks_actors_list = []

        genres_ids_list = []
        actors_ids_list = []

        for _, row in imdb.iterrows():
            film_rank = int(row['RANK'])

            genres_list = separate_elements(row['GENRE'])
            actors_list = separate_elements(row['ACTORS'])

            # Genres
            for genre in genres_list:
                film_ranks_genres_list.append(film_rank)
                genres_ids_list.append(int(genres_df[genres_df['GENRE_NAME'] == genre.strip().upper()]['GENRE_ID'].iloc[0]))

            # Actors
            for actor in actors_list:
                film_ranks_actors_list.append(film_rank)
                actors_ids_list.append(int(actors_df[actors_df['ACTOR_NAME'] == actor]['ACTOR_ID'].iloc[0]))

        bridge_film_genre = pd.DataFrame(list(zip(film_ranks_genres_list, genres_ids_list)), columns=['FILM_RANK', 'GENRE_ID'])
        bridge_film_actor = pd.DataFrame(list(zip(film_ranks_actors_list, actors_ids_list)), columns=['FILM_RANK', 'ACTOR_ID'])
        
        bridge_film_genre.to_csv(sep=d, index_label=['BRIDGE_GENRE_ID'], path_or_buf= (local_path + 'genres' + extension))
        bridge_film_actor.to_csv(sep=d, index_label=['BRIDGE_ACTOR_ID'], path_or_buf= (local_path + 'actors' + extension))

    bridge_film_genre = pd.read_csv(local_path + 'genres' + extension)
    bridge_film_actor = pd.read_csv(local_path + 'actors' + extension)

    return bridge_film_genre, bridge_film_actor

# Function transform and save/load data into the fact table
def get_fact_table(imdb: pd.DataFrame, bridge_film_genre: pd.DataFrame, bridge_film_actor: pd.DataFrame, years_df: pd.DataFrame, directors_df: pd.DataFrame, load_from_disk=False):
    
    local_path = f'{DATASETS_FOLDER}fact_table'
    d = ','
    h = 0
    extension = '.csv'

    if(not load_from_disk):
        fact_table = imdb[['RANK', 'RUNTIME', 'RATING', 'VOTES', 'REVENUE', 'METASCORE']]
        fact_table = fact_table.rename(columns={'RANK': 'FILM_RANK', 'RATING': 'USER_SCORE', 'VOTES': 'USER_VOTES', 'METASCORE': 'CRITIC_SCORE'})
        
        # Standaridize the rating between 1 and 100 (int values)
        fact_table = fact_table.astype({'CRITIC_SCORE': 'int32'})
        fact_table['USER_SCORE'] = fact_table['USER_SCORE'].map(lambda rating: rating * 10)
        fact_table = fact_table.astype({'USER_SCORE': 'int32'})

        # Categorize the numeric columns to nominal values
        fact_table['CRITIC_SCORE_CATEGORY'] = fact_table['CRITIC_SCORE'].apply(categorize_score)
        fact_table['USER_SCORE_CATEGORY'] = fact_table['USER_SCORE'].apply(categorize_score)
        fact_table['REVENUE_CATEGORY'] = fact_table['REVENUE'].apply(categorize_column, args=(fact_table['REVENUE'].min(), fact_table['REVENUE'].max()))
        fact_table['RUNTIME_CATEGORY'] = fact_table['RUNTIME'].apply(categorize_column, args=(fact_table['RUNTIME'].min(), fact_table['RUNTIME'].max()))
        fact_table['USER_VOTES_CATEGORY'] = fact_table['USER_VOTES'].apply(categorize_column, args=(fact_table['USER_VOTES'].min(), fact_table['USER_VOTES'].max()))

        bridge_genres_ids_list = []
        bridge_actors_ids_list = []
        years_ids_list = []
        directors_ids_list = []

        for _, row in imdb.iterrows():
            film_rank = int(row['RANK'])

            year = int(row['YEAR'])
            director = str(row['DIRECTOR']).strip()

            years_ids_list.append(int(years_df[years_df['YEAR'] == year]['YEAR_ID'].iloc[0]))
            directors_ids_list.append(int(directors_df[directors_df['DIRECTOR_NAME'] == director]['DIRECTOR_ID'].iloc[0]))

            bridge_genres_ids_list.append(int(bridge_film_genre[bridge_film_genre['FILM_RANK'] == film_rank]['BRIDGE_GENRE_ID'].iloc[0]))
            bridge_actors_ids_list.append(int(bridge_film_actor[bridge_film_actor['FILM_RANK'] == film_rank]['BRIDGE_ACTOR_ID'].iloc[0]))
            
        fact_table['BRIDGE_GENRE_ID'] = bridge_genres_ids_list
        fact_table['BRIDGE_ACTOR_ID'] = bridge_actors_ids_list
        fact_table['DIRECTOR_ID'] = directors_ids_list
        fact_table['YEAR_ID'] = years_ids_list

        #Reorder columns
        reordered_columns = ['FILM_RANK', 'BRIDGE_GENRE_ID', 'BRIDGE_ACTOR_ID', 'DIRECTOR_ID', 'YEAR_ID', 'RUNTIME', 'REVENUE', 'USER_VOTES', 'USER_SCORE', 'CRITIC_SCORE', 'RUNTIME_CATEGORY', 'REVENUE_CATEGORY', 'USER_VOTES_CATEGORY', 'USER_SCORE_CATEGORY', 'CRITIC_SCORE_CATEGORY']
        fact_table = fact_table[reordered_columns]

        fact_table.to_csv(sep=d, index=False, path_or_buf= (local_path + extension))

    fact_table = pd.read_csv(local_path + extension, delimiter=d, header=h)

    return fact_table

# -----------------------------------------------------------------------
#                                MAIN
# -----------------------------------------------------------------------

# Load local csv into Pandas Dataframe
local_data_path = f'{DATASETS_FOLDER}imdb_dataset.csv'
imdb = pd.read_csv(local_data_path, delimiter=',', header=0)

# Delete rows with null elements
imdb = imdb.dropna(how='any', axis=0)
print(imdb.info())

dim_columns = ['GENRE', 'ACTORS', 'YEAR', 'DIRECTOR']
bridge_columns = ['RANK', 'GENRE', 'ACTORS']

load_from_disk = False
genres_df, actors_df, years_df, directors_df = get_dims(imdb[dim_columns], load_from_disk)
bridge_film_genre, bridge_film_actor = get_bridge_tables(imdb[bridge_columns], genres_df, actors_df, load_from_disk)
fact_table = get_fact_table(imdb, bridge_film_genre, bridge_film_actor, years_df, directors_df, load_from_disk)

movie_info_table = imdb[['RANK', 'TITLE', 'DESCRIPTION']]
movie_info_table.rename(columns={'RANK': 'FILM_RANK'})
movie_info_table.to_csv(sep=',', path_or_buf=f'{DATASETS_FOLDER}movie_info_table.csv', index=False)

print(actors_df.shape)
print(bridge_film_actor.shape)
print(fact_table.shape)
