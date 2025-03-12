"""
File: filter_pushshift_comments.py

Author: Anjola Aina
Date Modified: March 12th, 2025

Description:
    This file contains all the necessary functions used to decompress zst zipped files containing reddit data (represented as a JSON file).
    
    To extract specific comments from subreddits, it is highly recommended to use the following source: https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/combine_folder_multiprocess.py. 
    This file allows one to specify via the command line the number of subreddits to extract from, along with other values such as the author.
    
    To extract all zipped zst folders, the following source from StackOverflow was used as a reference: https://stackoverflow.com/questions/31346790/unzip-all-zipped-files-in-a-folder-to-that-same-folder-using-python-2-7-5
"""

import argparse
import json
import os
import re
import sys
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

def matches_keyword(comment: list[str], keyword: str, loose_match: bool = False):
    """
    Checks if a comment contains the specified keyword. 
    
    If the keyword is a single word, it will only check for an exact match.
    Otherwise, it checks for an exact match before checking if all words in the phrase appear somewhere in the comment.
    comments based on the domain keywords and domain feature keywords.

    Args:
        comment (list[str]): A Reddit comment.
        keyword (str): The keyword to check if it exists in the comment.
        loose_match (bool, optional): Determines whether to check for a loose match or not. Defaults to False.

    Returns:
        bool: True if the comment contains the keyword (through exact or loose matching), False otherwise.
    """
    comment = comment.lower()
    keyword = keyword.lower()
    
    # Exact match first
    if keyword in comment:
        return True
    
    # Loose match: Check if all words in the phrase appear somewhere in the comment
    if loose_match:
        words = keyword.split()
        return all(word in comment for word in words)
    else:
        return False # return False as exact matching failed

def filter_comments(comments_list: dict, keywords: list[str], loose_match: bool = False) -> list[str]:
    """
    Filters comments based on keywords.

    Args:
        comments_list (dict): The dictionary containing all non bot-generated comments with their corresponding id.
        keywords (list[str]): The list of keywords that a comment should contain.
        loose_match (bool, optional): Determines whether to check for a loose match or not. Defaults to False.

    Returns:
        list[str]: The list of filtered comments.
    """
    filtered_comments = []
    for comment in comments_list:
        # Add comment if it matches any of the supplied keywords
        for keyword in keywords:
            if matches_keyword(comment, keyword, loose_match):
                filtered_comments.append(comment)
        
    return filtered_comments

def process_comments_from_file(file_path: str, bot_author: str='AutoModerator') -> list[str]:
    """
    Processes all comments from a single file. Bot-generated comments and 'daily thread' comments are not appended.
    
    Args:
        file_path(str): The path of the file containing comments from a specific subreddit.
        bot_author(str): The bot author to filter out bot-generated comments. Defaults to AutoModerator.
        
    Returns:
        list(str): The list of comments.
    """
    comments = []
    
    with open(file=file_path, mode='r', encoding='utf-8') as fp:
        for line in fp:
            try:
                data = json.loads(line)
                comment = data['body']
                if data['author'] != bot_author and 'daily threads' not in comment.lower(): # Skip bot-generated comments
                    comment_parts = split_on_newlines(comment)
                    for part in comment_parts:
                            comments.append(part)
            except json.JSONDecodeError:
                print(f'Error decoding JSON in file: {file_path}')
                
    return comments

def process_comments_from_folder(folder_path: str) -> list[str]:
    """
    Processes all comments from a single folder. Bot-generated comments and 'daily thread' comments are not appended.
    
    Args:
        folder_path(str): The path of the folder containing files with comments from specific subreddits.
        
    Returns:
        list(str): The list of filtered comments.   
    """
    all_comments = [] 
    for file in tqdm(os.listdir(folder_path), desc='Processing comments...'):
        file_path = os.path.join(folder_path, file) # Create full path for the file
        comments = process_comments_from_file(file_path)
        all_comments.extend(comments)
    
    return all_comments

    
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
    
    os.chdir(dir_path)
    
    output_dir = os.path.abspath(output_path)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    
    comments_dir = os.path.join(output_dir, 'comments')
    if not os.path.exists(comments_dir):
        os.makedirs(comments_dir)
        
    # Unzip all zipped comments in the directory
    for item in tqdm(os.listdir(dir_path), desc='Extracting files...'):
        
        if item.endswith(extension):
            zst_file_path = os.path.join(dir_path, item)
            
            # Determine output path based on file type
            if '_comments.zst' in item:
                output_file_path = os.path.join(comments_dir, item.replace('.zst', ''))
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
    output_path = 'C:\\Users\\anjol\\Desktop\\reddit_data\\comments_2024_09'
    decompress_zst_files(dir_path, output_path)

    comments_dir = os.path.join(output_path, 'comments')
    
    # Extracting relevant comments
    json_data = process_comments_from_folder(comments_dir)
    # Filter comments
    json_data = filter_comments(json_data, ["keyword"])
    
    # Writing the JSON file to the output directory
    json_path = os.path.join(output_path, 'filtered_comments.json')
    with open(json_path, mode='w', encoding='utf-8') as out_file: 
        for line in json_data:
            out_file.write(line + '\n')


def _list_of_strings(arg):
    return arg.split(',')

if __name__ == '__main__':
    # Creating parser and adding arguments
    parser = argparse.ArgumentParser(description='This script extracts and filter comments from Pushshift dumps.\nThe input folder should contain the zst folder containing all relevant subreddits from Pushshift dumps.\nThe extracted and filtered comments will be placed in the specified output folder.\nThe --keywords option allows you to specify specific keywords that comments should contain. They should be comma separated values, such as: meal planning,calories,macro tracking\nThe --loose_match flag enables loose matching, which check if all words in the keyword appear somewhere in a comment.')
    
    parser.add_argument('input', type=str, required=True, help='Input directory containing Pushshift data.')
    parser.add_argument('--output', type=str, required=False, help='Output directory for filtered results (defaults to input directory).')
    parser.add_argument('--keywords', type=_list_of_strings, required=False, help='Keywords to filter out the extracted comments. Supports a comma separated list. Case insensitive.')
    parser.add_argument('--loose_match', type=bool, action='store_true', required=False, help='Enables loose matching, which check if all words in the keyword appear somewhere in a comment.')
    
    # Parse arguments     
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    
    # Default output to input directory if not provided
    output_dir = args.output if args.output else args.input
    
    try:
        decompress_zst_files(args.input, output_dir)
    except NameError:
        sys.exit(1) # The input directory does not exist, stop execution
    
    comments_dir = os.path.join(output_dir, 'comments')
        
    # Extracting relevant comments
    json_data = process_comments_from_folder(comments_dir)
    # Filter comments
    json_data = filter_comments(json_data, ["keyword"])
        
    # Writing the JSON file to the output directory
    json_path = os.path.join(output_dir, 'filtered_comments.json')
    with open(json_path, mode='w', encoding='utf-8') as out_file: 
        for line in json_data:
            out_file.write(line + '\n')
   