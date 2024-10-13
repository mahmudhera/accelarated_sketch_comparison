import argparse
import pandas as pd
import json


def get_name_from_json_file(file_path):
    with open(file_path) as f:
        data = json.load(f)
    return data[0]['name']


def parse_args():
    # arguments: multisearch_output frackmc_output filelist
    parser = argparse.ArgumentParser(description='Test if outputs in multisearch_output exist in frackmc_output')
    parser.add_argument('multisearch_output', help='Path to multisearch output file')
    parser.add_argument('frackmc_output', help='Path to frackmc output file')
    parser.add_argument('filelist', help='Path to filelist file')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # read multisearch output as a dataframe
    # the column names: query_name,query_md5,match_name,match_md5,containment,max_containment,jaccard,intersect_hashes
    multisearch_df = pd.read_csv(args.multisearch_output, sep=',')

    # read the filelist file, each line is a file path
    with open(args.filelist) as f:
        file_path_list = f.read().splitlines()

    # read each file in file_path_list, and get the name from the file
    name_list = [ get_name_from_json_file(file_path) for file_path in file_path_list ]

    # read the frackmc output as a dataframe
    # the column names: file_index1, file_index2, jaccard
    frackmc_df = pd.read_csv(args.frackmc_output, sep=' ', header=None, names=['file_index1', 'file_index2', 'jaccard'])

    num_files = len(file_path_list)

    for file_index1 in range(num_files):
        # find the number of rows in frackmc_df where file_index1 is the same as file_index1
        rows = frackmc_df[frackmc_df['file_index1'] == file_index1]
        num_rows_frackmc = len(rows)

        # get the number of rows in multisearch_df where query_name is the same as name_list[file_index1], and jaccard > 0.1
        rows = multisearch_df[(multisearch_df['query_name'] == name_list[file_index1]) & (multisearch_df['jaccard'] > 0.1)]
        num_rows_multisearch = len(rows)

        # check if these two numbers are the same
        print( file_index1, name_list[file_index1], num_rows_frackmc, num_rows_multisearch )
        if num_rows_frackmc != num_rows_multisearch:
            print('Error: file_index1 =', file_index1, 'num_rows_frackmc =', num_rows_frackmc, 'num_rows_multisearch =', num_rows_multisearch)




if __name__ == '__main__':
    main()