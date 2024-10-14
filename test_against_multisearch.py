import argparse
import pandas as pd
from tqdm import tqdm

def parse_args():
    # arguments: multisearch_file, by_index_file, genome_names
    parser = argparse.ArgumentParser(description='Test against multisearch')
    parser.add_argument('multisearch_file', type=argparse.FileType('r'))
    parser.add_argument('by_index_file', type=argparse.FileType('r'))
    parser.add_argument('genome_names', type=argparse.FileType('r'))
    
    return parser.parse_args()

def read_genome_names(genome_names):
    return [name.strip() for name in genome_names]

def read_multisearch(multisearch_file):
    # a csv file with columns: query_name,query_md5,match_name,match_md5,containment,max_containment,jaccard,intersect_hashes
    df = pd.read_csv(multisearch_file, sep=',')
    return df

def read_by_index(by_index_file):
    # a csv file, columns are: query_id, match_id, jaccard, containment, containment_other, no header
    df = pd.read_csv(by_index_file, sep=',', header=None)
    df.columns = ['query_id', 'match_id', 'jaccard', 'containment', 'containment_other']
    return df

def test_against_multisearch(multisearch_file, by_index_file, genome_names):
    df_multisearch = read_multisearch(multisearch_file)
    df_by_index = read_by_index(by_index_file)
    genome_names = read_genome_names(genome_names)
    
    # iterate over all rows of the by_index file
    for index, row in tqdm(df_by_index.iterrows(), total=df_by_index.shape[0]):
        query_id = int(row['query_id'])
        match_id = int(row['match_id'])
        jaccard = row['jaccard']
        containment = row['containment']
        containment_other = row['containment_other']
        max_containment_by_index = max(containment, containment_other) 
        
        # get the corresponding row in the multisearch file
        multisearch_row = df_multisearch[(df_multisearch['query_name'] == genome_names[query_id]) & (df_multisearch['match_name'] == genome_names[match_id])]
        
        multisearch_jaccard = multisearch_row['jaccard'].values[0]
        multisearch_containment = multisearch_row['containment'].values[0]
        multisearch_max_containment = multisearch_row['max_containment'].values[0]
        
        # check if the jaccard and containment values are the same upto 3 decimal places
        assert abs(jaccard - multisearch_jaccard) < 0.001
        assert abs(containment - multisearch_containment) < 0.001
        assert abs(max_containment_by_index - multisearch_max_containment) < 0.001
        
    print('All tests passed!')
        
def main():
    args = parse_args()
    print('Testing for ...')
    # show the arguments
    print(args)
    test_against_multisearch(args.multisearch_file, args.by_index_file, args.genome_names)
    
if __name__ == '__main__':
    main()