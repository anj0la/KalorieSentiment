"""
File: filter_pushshift_data.py

Author: Anjola Aina
Date Modified: September 25th, 2024

Description:
    This file contains all the necessary functions used to decompress zst zipped files containing reddit data (represented as a JSON file).
    It is assumed that the files have been extracted to a dir_path. 
    
    To extract specific comments and submissions from subreddits, it is highly recommended to use the following source: https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/combine_folder_multiprocess.py. 
    This file allows one to specify via the command line the number of subreddits to extract from, along with other values such as the author.
    
    To extract all zipped zst folders, the following source from StackOverflow was used as a reference: https://stackoverflow.com/questions/31346790/unzip-all-zipped-files-in-a-folder-to-that-same-folder-using-python-2-7-5
"""

from filter_constants import DOMAIN_KEYWORDS, DOMAIN_FEATURE_KEYWORDS
import json
import os
import re
from tqdm import tqdm
import zstandard as zstd

def split_on_newlines(comment: str) -> list[str]:
    """
    Splits the comment on single or multiple newlines, removing leading/trailing spaces in each part.

    Args:
        comment (str): The comment to be split into multiple comments.

    Returns:
        list[str]: The new list of comments from the single comment containing multiple newlines.
    """
    return [part.strip() for part in re.split(r'\n+', comment) if part.strip()]

def contains_phrase(comment: str, phrases: list[str]) -> bool:
    """
    Checks if a comment contains a phrase from list of phrases.
    
    This is achieved by creating a regex pattern that matches whole words (including single-word and multi-word phrases).

    Args:
        comment (str): The comment.
        phrases (list[str]): The list of phrases that the commment must contain.

    Returns:
        bool: True if the comment contains a phrase, False otherwise.
    """
    for phrase in phrases:
        # Create a regex pattern to match whole words (handles single-word and multi-word phrases)
        pattern = r'\b' + re.escape(phrase) + r'\b'
        if re.search(pattern, comment, flags=re.IGNORECASE):
            return True
    return False

def filter_comments(data: dict, domain_keywords: list[str], domain_feature_keywords: list[str]) -> list[str]:
    """
    Filters comments based on the domain keywords and domain feature keywords.

    Args:
        data (dict): The dictionary containing all non bot-generated comments with their corresponding id.
        domain_keywords (list[str]): The list of domain keywords that a comment should contain.
        domain_feature_keywords (list[str]): The list of domain feature keywords that a comment should contain.

    Returns:
        list[str]: _description_
    """
    filtered_data = []
    for entry in data:
        comment = entry

        # Check if any domain keywords and feature keywords match
        if (contains_phrase(comment, domain_keywords) and 
            contains_phrase(comment, domain_feature_keywords)):
            filtered_data.append(entry)

    return filtered_data

def process_comments_from_file(file_path: str, bot_author: str='AutoModerator') -> list[dict]:
    """
    Processes all comments from a single file. Bot-generated comments and 'daily thread' comments are not appended.
    
    Args:
        file_path(str): The path of the file containing comments from a specific subreddit.
        bot_author(str): The bot author to filter out bot-generated comments. Defaults to AutoModerator.
        
    Returns:
        list(dict): The list of filtered comments.
    """
    comments_list = []
    
    with open(file=file_path, mode='r', encoding='utf-8') as fp:
        for line in fp:
            try:
                data = json.loads(line)
                comment = data['body']
                # Skip bot-generated comments
                if data['author'] != bot_author and 'daily threads' not in comment.lower():
                    comment_parts = split_on_newlines(comment)
                    for part in comment_parts:
                            comments_list.append(part)
            except json.JSONDecodeError:
                print(f'Error decoding JSON in file: {file_path}')
                
    # Filter the comments after adding them
    sorted_feature_keywords = sorted(DOMAIN_FEATURE_KEYWORDS, key=lambda x: len(x.split()), reverse=True)
    filtered_comments = filter_comments(comments_list, DOMAIN_KEYWORDS, sorted_feature_keywords)
    return filtered_comments
  
"""              
def process_submission(file_path: str) -> tuple[list[str], list[str], list[dict]]:
    comments_list = []
    
    with open(file=file_path, mode='r', encoding='utf-8') as fp:
        for line in fp:
            try:
                data = json.loads(line)
                submission = data['selftext']
                if any(keyword in submission for keyword in DOMAIN_KEYWORDS) and any(keyword in submission for keyword in DOMAIN_FEATURE_KEYWORDS):
                    comments_list.append({'comment': submission})
            except json.JSONDecodeError:
                print(f'Error decoding JSON in file: {file_path}')
    
    return comments_list
"""

def process_comments_from_folder(folder_path: str) -> list[dict]:
    """
    Processes all comments from a single file. Bot-generated comments and 'daily thread' comments are not appended.
    
    Args:
        folder_path(str): The path of the folder containing files with comments from specific subreddits.
        
    Returns:
        list(dict): The list of filtered comments.   
    """
    all_comments = [] 
    for file in tqdm(os.listdir(folder_path), desc='Processing comments...'):
        # Get the comments, apps and features and add it to the json data
        file_path = os.path.join(folder_path, file) # Create full path for the file
        comments = process_comments_from_file(file_path)
        all_comments.extend(comments)
    
    return all_comments

""" def process_all_submissions(folder_path: str) -> list[dict]: 
    all_submissions = []
    for file in tqdm(os.listdir(folder_path), desc='Processing submissions...'):
        file_path = os.path.join(folder_path, file) # Create full path for the file
        # Get the comments, apps and features and add it to the json data
        submissions = process_submission(file_path)
        all_submissions.extend(submissions)
        
    return all_submissions

"""
    
def decompress_zst_files(dir_path: str, output_path: str, extension: str = '.zst') -> None:
    """
    Extracts all zip files in the specified folder path into the output path.
    If the output path does not exist, the directory is made.

    Args:
        dir_path (str): The path of the folder containing the zipped files.
        output_path (str): The output file to store all unzipped files.
        extension (str, optional): The extension. Defaults to '.zst'.
        
    Raises:
        NameError: Occurs when the dir_path does not exist.
    """
    # If the directory containing the path doesn't exist, extraction cannot occur
    if not os.path.exists(dir_path):
        raise NameError(f'{dir_path} does not exist.')
    
    # Change the directory to the path containing the zipped folders
    os.chdir(dir_path)

    # Create a directory to store all unzipped submissions and comments
    output_dir = os.path.abspath(output_path)
    
    # Check if the directory exists, if not, create it
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    
    # Create two directories to store comments and submissions
    comments_dir = os.path.join(output_dir, 'comments')
    submissions_dir = os.path.join(output_dir, 'submissions')
        
    # Check if the directories exist, if not, create them
    if not os.path.exists(comments_dir):
        os.makedirs(comments_dir)
    if not os.path.exists(submissions_dir):
        os.makedirs(submissions_dir)
        
    # Unzip all zipped submissions/comments in the directory
    for item in tqdm(os.listdir(dir_path), desc='Extracting files...'):
        
        # Collect all items with the .zat extension
        if item.endswith(extension):
            zst_file_path = os.path.join(dir_path, item)
            
            # Determine output path based on file type (comments or submissions)
            if '_comments.zst' in item:
                output_file_path = os.path.join(comments_dir, item.replace('.zst', ''))
            elif '_submissions.zst' in item:
                output_file_path = os.path.join(submissions_dir, item.replace('.zst', ''))
            else:
                print(f'Skipping {item} as it doesn\'t match expected file patterns.')
                continue
            
            # Decompress the .zst file
            with open(zst_file_path, 'rb') as compressed_file:
                dctx = zstd.ZstdDecompressor()
                with open(output_file_path, 'wb') as output_file:
                    dctx.copy_stream(compressed_file, output_file)
                
def main():
    # Unzipping all files in the specified directory into the output path
    dir_path = 'C:\\Users\\anjol\\Desktop\\reddit_data\\f_comments'
    output_path = 'C:\\Users\\anjol\\Desktop\\reddit_data\\comments_2024_06'
    # decompress_zst_files(dir_path, output_path)

    comments_dir = os.path.join(output_path, 'comments')
    # submissions_dir = os.path.join(output_path, 'submissions')
    
    # Extracting relevant comments
    json_data = process_comments_from_folder(comments_dir)
    # json_data.extend(process_all_submissions(submissions_dir))
    
    # Writing the JSON file to the output directory
    json_path = os.path.join(output_path, 'filtered_comments_2024_06.json')
    with open(json_path, mode='w', encoding='utf-8') as out_file: 
        for line in json_data:
            out_file.write(line + '\n')

if __name__ == '__main__':
   main()
