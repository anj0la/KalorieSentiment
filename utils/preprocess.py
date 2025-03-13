"""
File: preprocess.py

Author: Anjola Aina
Date Modified: March 13th, 2025

This file contains all the necessary functions used to preprocess the collected data.
"""
import emoji
import pandas as pd
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

def clean_input(comments: list | dict) -> list:
    # Error checking
    if not comments:
        raise ValueError('Expected list or dict of comments.')    
    
    # Convert comments list to Pandas DataFrame for easier manipulation
    df = pd.DataFrame(comments, colums='comment')
    data = df['comment']
    
    # Convert the text to lowercase
    data = data.str.lower()
    
    # Remove Unicode characters (non-ASCII)
    data = data.apply(lambda x: x.encode('ascii', 'ignore').decode('ascii'))
    
    # Remove punctuation, special characters, emails and links
    data = data.replace(r'[^\w\s]', '', regex=True)  # Removes non-alphanumeric characters except whitespace
    data = data.replace(r'http\S+|www\.\S+', '', regex=True)  # Remove URLs
    data = data.replace(r'\w+@\w+\.com', '', regex=True)  # Remove emails
    
    # Convert emojis to text
    data = data.apply(lambda x: emoji.demojize(x))
    data = data.replace(r':(.*?):', '', regex=True)
        
    # Remove stop words and apply lemmatization
    stop_words = set(stopwords.words('english'))
    data = data.apply(lambda sentence: ' '.join(WordNetLemmatizer().lemmatize(word) for word in sentence.split() if word not in stop_words))
    
    return data.values

