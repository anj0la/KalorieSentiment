"""
File: filter_pushshift_comments.py

Author: Anjola Aina
Date Modified: March 13th, 2025

Description:
    This script extracts and filters Reddit comments from decompressed `.zst` Pushshift data files. It first processes files to extract all comments before 
    filtering them based on specified keywords (if provided). Afterwards, it saves the extracted or filtered comments into a single JSON file. If the output
    directory is not given, it is saved inside of the input directory.
    
    For subreddit-specific extraction, use `combine_folder_multiprocess.py` from Watchful1.  
    - Repository: https://github.com/Watchful1/PushshiftDumps  
    - Individual file/folder extraction: Use `single_file` and `iterate_folder` scripts from Watchful1.  

Features:
    - It processes files to extract all comments.  
    - Filters comments based on specified keywords (if provided).  
    - Saves the extracted or filtered comments in a JSON file.  
    
Usage Examples:
    - Extract all comments from the input directory, keeping comments with the keywords meal planning (exact and loose match) and save them to 'filtered_comments.json' in the output directory:
      ```
      python filter_pushshift_comments.py input_dir --output out_dir --keywords meal planning --loose_match
      ```
    
    - Extract all comments from the input directory, keeping comments with the keywords customization and goals (exact match) and save them to 'filtered_comments.json' in the current directory:
      ```
      python filter_pushshift_comments.py input_dir --keywords customization,goals
      ```
Attribution:
    - The batch decompression method for .zst files is based on this Stack Overflow post:
    https://stackoverflow.com/questions/31346790/unzip-all-zipped-files-in-a-folder-to-that-same-folder-using-python-2-7-5

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
    
    for comment in tqdm(comments_list, desc='Filtering comments...'):
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
        file_path = os.path.join(folder_path, file)
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
    
    dir_path = os.path.abspath(dir_path)  # Convert to absolute path

    if not os.path.exists(dir_path):
        raise NameError(f'{dir_path} does not exist.')
    
    parent_dir = os.path.dirname(dir_path)
    output_dir = os.path.join(parent_dir, os.path.basename(output_path))

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

def _list_of_strings(arg: str) -> list[str]:
    """
    Splits a string with comma separated values into a list of strings.

    Args:
        arg (str): The string with comma separated values.

    Returns:
        list[str]: The strings as a list.
    """
    return arg.split(',')

if __name__ == '__main__':
    # Creating parser and adding arguments
    parser = argparse.ArgumentParser(description='This script extracts and filter comments from Pushshift dumps.\nThe input folder should contain the zst folder containing all relevant subreddits from Pushshift dumps.\nThe extracted and filtered comments will be placed in the specified output folder.\nThe --keywords option allows you to specify specific keywords that comments should contain. They should be comma separated values, such as: meal planning,calories,macro tracking\nThe --loose_match flag enables loose matching, which check if all words in the keyword appear somewhere in a comment.')
    
    parser.add_argument('input', type=str, help='Input directory containing Pushshift data.')
    parser.add_argument('--output', type=str, required=False, help='Output directory for extracted/filtered results (defaults to input directory).')
    parser.add_argument('--json_file_name', type=str, required=False, help='Name of the final file containing the extracted/filtered comments.')
    parser.add_argument('--keywords', type=_list_of_strings, required=False, help='Keywords to filter out the extracted comments. Supports a comma separated list. Case insensitive.')
    parser.add_argument('--loose_match', action='store_true', required=False, help='Enables loose matching, which check if all words in the keyword appear somewhere in a comment.')
    
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    
    output_dir = args.output if args.output else args.input # output_dir = input_dir if not provided
    
    try:
        decompress_zst_files(args.input, output_dir)
    except NameError:
        sys.exit(1) # The input directory does not exist, stop execution
    
    comments_dir = os.path.join(output_dir, 'comments')
        
    json_data = process_comments_from_folder(comments_dir)
    if args.keywords:
        json_data = filter_comments(json_data, args.keywords)
        
    json_path = os.path.join(output_dir, 'extracted_comments.json' if not args.json_file_name else args.json_file_name + '.json')
    with open(json_path, mode='w', encoding='utf-8') as out_file: 
        for line in json_data:
            out_file.write(line + '\n')
   