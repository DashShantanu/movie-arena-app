import pandas as pd
import numpy as np

from tmdbv3api import TMDb
from tmdbv3api import Movie
import json
import requests
import os

tmdb = TMDb()
tmdb_movie = Movie()
tmdb.api_key = os.environ['API_KEY']


# Get the director's name from the 'Cast and crew' column
def get_director(x):
    if isinstance(x, str):
        if " (director)" in x:
            return x.split(" (director)")[0]
        elif " (directors)" in x:
            return x.split(" (directors)")[0]
        elif " (director/screenplay)" in x:
            return x.split(" (director/screenplay)")[0]
    return np.NaN


# generic function to get data from wikipedia, when input a year
def get_data(year):
    link = "https://en.wikipedia.org/wiki/List_of_American_films_of_" + \
        str(year)

    dfs = pd.read_html(link, header=0)

    if len(dfs) >= 6:
        df1 = dfs[2]
        df2 = dfs[3]
        df3 = dfs[4]
        df4 = dfs[5]

        # Combine DataFrames vertically
        df = pd.concat([df1, df2, df3, df4], ignore_index=True)

        # Define the get_genre function
        def get_genre(x):
            genres = []
            result = tmdb_movie.search(x)
            if len(result) > 0:
                movie_id = result[0].id
                response = requests.get(
                    'https://api.themoviedb.org/3/movie/{}?api_key={}'.format(movie_id, tmdb.api_key))
                data_json = response.json()
                if data_json['genres']:
                    genre_str = " "
                    for i in range(0, len(data_json['genres'])):
                        genres.append(data_json['genres'][i]['name'])
                    return genre_str.join(genres)
            return np.NaN

        # Apply the get_genre function to the 'Title' column
        df['genres'] = df['Title'].map(lambda x: get_genre(str(x)))

        # Print the resulting DataFrame
        print(df)
    else:
        print("Not enough DataFrames found on the page.")

    # Filter out required columns
    df_filter = df[['Title', 'Cast and crew', 'genres']]

    # Add a new column for the director's name
    df_filter['director_name'] = df_filter['Cast and crew'].map(
        lambda x: get_director(x))

    #  Drop the 'Cast and crew' column and add actor_1_name, actor_2_name, actor_3_name columns
    def get_actor1(x):
        if isinstance(x, str):
            split_values = x.split("screenplay); ")
            if len(split_values) > 1:
                return split_values[-1].split(", ")[0]
        return np.NaN

    df_filter['actor_1_name'] = df_filter['Cast and crew'].map(
        lambda x: get_actor1(x))

    def get_actor2(x):
        if isinstance(x, str):
            split_values = x.split("screenplay); ")
            if len(split_values) > 1 and len(split_values[-1].split(", ")) >= 2:
                return split_values[-1].split(", ")[1]
        return np.NaN

    df_filter['actor_2_name'] = df_filter['Cast and crew'].map(
        lambda x: get_actor2(x))

    def get_actor3(x):
        if isinstance(x, str):
            split_values = x.split("screenplay); ")
            if len(split_values) > 1 and len(split_values[-1].split(", ")) >= 3:
                return split_values[-1].split(", ")[2]
        return np.NaN

    df_filter['actor_3_name'] = df_filter['Cast and crew'].map(
        lambda x: get_actor3(x))

    #  rename the column 'Title' to 'movie_title'
    df_filter = df_filter.rename(columns={'Title': 'movie_title'})

    # keep only the required columns
    new_df_filter = df_filter.loc[:, [
        'director_name', 'actor_1_name', 'actor_2_name', 'actor_3_name', 'genres', 'movie_title']]

    # Fill NaN values with 'unknown'
    new_df_filter['actor_2_name'] = new_df_filter['actor_2_name'].replace(
        np.nan, 'unknown')
    new_df_filter['actor_3_name'] = new_df_filter['actor_3_name'].replace(
        np.nan, 'unknown')

    new_df_filter['movie_title'] = new_df_filter['movie_title'].str.lower()

    new_df_filter['comb'] = new_df_filter['actor_1_name'] + ' ' + new_df_filter['actor_2_name'] + ' ' + \
        new_df_filter['actor_3_name'] + ' ' + \
        new_df_filter['director_name'] + ' ' + new_df_filter['genres']

    # check for null values
    new_df_filter.isna().sum()

    #  remove the rows where there are null values
    new_df_filter = new_df_filter.dropna(how='any')
    new_df_filter.isna().sum()

    # convert to csv file
    new_df_filter.to_csv('new_data'+str(year)+'.csv', index=False)
