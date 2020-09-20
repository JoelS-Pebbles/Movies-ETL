#!/usr/bin/env python
# coding: utf-8

# In[31]:


import pandas as pd
import numpy as np
import json
import os
import re

from sqlalchemy import create_engine
import psycopg2

from config import db_password

import time


# In[62]:


def clean_movie(movie):
    movie = dict(movie) 
    alt_titles = {}   
    for alt_title_key in ["Also known as", "Arabic", "Cantonese", "Chinese", "French", 
                          "Hangul", "Hebrew", "Hepburn", "Japanese", "Literally", 
                          "Mandarin", "McCune–Reischauer", "Original title", "Polish", 
                          "Revised Romanization", "Romanized", "Russian", "Simplified", 
                          "Traditional", "Yiddish"]:
    
        if alt_title_key in movie:
            
            alt_titles[alt_title_key] = movie[alt_title_key]
            movie.pop(alt_title_key)
    
    if len(alt_titles) > 0:
        movie["alt_titles"] = alt_titles
    
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name("Directed by", "Director")
    change_column_name("Country of origin", "Country")
    change_column_name("Distributed by", "Distributor")
    change_column_name("Edited by", "Editor(s)")
    change_column_name("Music by", "Composer(s)")
    change_column_name("Produced by", "Producer(s)")
    change_column_name("Producer", "Producer(s)")
    change_column_name("Directed by", "Director")
    change_column_name("Productioncompany ", "Production company(s)")
    change_column_name("Productioncompanies ", "Production company(s)")
    change_column_name("Original release", "Release date")
    change_column_name("Released", "Release date")
    change_column_name("Length", "Running time")
    change_column_name("Theme music composer", "Composer(s)")
    change_column_name("Adaptation by", "Writer(s)")
    change_column_name("Screen story by", "Writer(s)")
    change_column_name("Screenplay by", "Writer(s)")
    change_column_name("Story by", "Writer(s)")
    change_column_name("Written by", "Writer(s)")

    return movie


# In[63]:


file_dir=r"C:\Users\12109\Documents\Data boot camp\Class Work\Module 8"


# In[64]:


def ETL():
    kaggle_metadata=pd.read_csv(f"{file_dir}\\movies_metadata.csv")
    ratings=pd.read_csv(f"{file_dir}\\ratings.csv")
    with open (f'{file_dir}/wikipedia-movies.json',mode='r')as file:
        wiki_movies_raw=json.load(file)   
        wiki_movies = [movie for movie in wiki_movies_raw                    if ('Director' in movie or 'Directed by' in movie)                    and 'imdb_link' in movie                    and "No. of episodes" not in movie]

    cleaned_wiki_movies = [clean_movie(movie) for movie in wiki_movies]

    cleaned_wiki_movies_df = pd.DataFrame(cleaned_wiki_movies)

    try:
        cleaned_wiki_movies_df["imdb_id"] = cleaned_wiki_movies_df['imdb_link'].str.extract(r"(tt\d{7})")
        cleaned_wiki_movies_df.drop_duplicates(subset="imdb_id", inplace=True)
    except Exception as e: print(e)

    non_null_columns = [column for column in cleaned_wiki_movies_df.columns                         if cleaned_wiki_movies_df[column].isnull().sum() < (0.9 * len(cleaned_wiki_movies_df))]
    wiki_movies_df = cleaned_wiki_movies_df[non_null_columns]
    
    box_office = wiki_movies_df["Box office"].dropna()
    
    box_office = box_office.apply(lambda x: ' '.join(x) if x == list else x)

    form_one = r"\$\s*\d+\.?\d*\s*[mb]illi?on"
    
    form_two = r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)"

    def parse_dollars(s):
        if type(s) != str:
            return np.nan
    
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            s = re.sub('\$|\s|[a-zA-Z]', '', s)
            value = float(s) * 10**6
            return value
    
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            s = re.sub('\$|\s|[a-zA-Z]', '', s)
            value = float(s) * 10**9
            return value
    
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):    
            s = re.sub('\$|,', '', s)
            value = float(s)
            return value
    
        else:
            return np.nan
    
        
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})',                                                           flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    budget = wiki_movies_df['Budget'].dropna().apply(lambda x: ' '.join(x) if x == list else x)
    budget = budget.str.replace(r'\$.*[---–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})',                                                   flags=re.IGNORECASE)[0].apply(parse_dollars)
    
    release_date = wiki_movies_df["Release date"].dropna().apply(lambda x: " ".join(x) if type(x) == list else x)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.        extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: " ".join(x) if type(x) == list else x)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    return wiki_movies_df, kaggle_metadata, ratings


# In[65]:


wiki_file = f'{file_dir}/wikipedia.movies.json'
kaggle_file = f'{file_dir}/movies_metadata.csv'
ratings_file = f'{file_dir}/ratings.csv'


# In[66]:


wiki_file, kaggle_file, ratings_file = ETL()


# In[67]:


wiki_movies_df = wiki_file


# In[68]:


wiki_movies_df.head()


# In[69]:


wiki_movies_df.columns.to_list()


# In[ ]:




