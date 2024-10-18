import pandas as pd 
import argparse 


def parse_args():
    # arguments: yacht_train_file by_index_file
    parser = argparse.ArgumentParser(description='Compare two files by index')
    parser.add_argument('yacht_train_file', type=str, help='Path to the yacht_train file')
    parser.add_argument('by_index_file', type=str, help='Path to the by_index file')
    return parser.parse_args()


def compare_yacht_train_by_index(yacht_train_file, by_index_file):
    # read the yacht_train file
    yacht_train_df = pd.read_csv(yacht_train_file, sep='\t')
    yacht_selected_set = set(yacht_train_df['md5sum'])

    # read the by_index file: all lines
    by_index_lines = []
    with open(by_index_file, 'r') as f:
        by_index_lines = f.read().splitlines()

    by_index_lines = [ line.split('/')[-1] for line in by_index_lines ]
    by_index_lines = [line.replace('.sig', '') for line in by_index_lines]
    by_index_set = set(by_index_lines)

    # compare the two sets
    print('Files in yacht_train but not in by_index:')
    print(yacht_selected_set - by_index_set)

    print('Files in by_index but not in yacht_train:')
    print(by_index_set - yacht_selected_set)

if __name__ == '__main__':
    args = parse_args()
    compare_yacht_train_by_index(args.yacht_train_file, args.by_index_file)
    