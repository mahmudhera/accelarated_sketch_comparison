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
    
    # create an index of the genome names
    genome_name_to_id = {}
    for i in range(len(genome_names)):
        genome_name_to_id[genome_names[i]] = i
    
    # iterate over all rows of the by_index file
    num_genomes = len(genome_names)
    for i in tqdm(range(num_genomes)):
        name_query_genome = genome_names[i]
        multisearch_results_with_this_query = df_multisearch[df_multisearch['query_name'] == name_query_genome]
        by_index_results_with_this_query = df_by_index[df_by_index['query_id'] == i]
        
        # create a dictionary of the by_index_results, keyed by match_id
        by_index_results_dict = {}
        for index, row in by_index_results_with_this_query.iterrows():
            match_id = int(row['match_id'])
            jaccard = row['jaccard']
            containment = row['containment']
            by_index_results_dict[match_id] = (jaccard, containment)
            
        # iterate over all rows of the multisearch file
        for index, row in multisearch_results_with_this_query.iterrows():
            match_name = row['match_name']
            match_id = genome_name_to_id[match_name]
            
            if match_id == i:
                continue
            
            jaccard_multisearch = row['jaccard']
            containment_multisearch = row['containment']
            
            # get the jaccard and containment from the by_index results
            jaccard_by_index, containment_by_index = by_index_results_dict[match_id]
            
            # compare the jaccard and containment values
            assert abs(jaccard_multisearch - jaccard_by_index) < 1e-4
            assert abs(containment_multisearch - containment_by_index) < 1e-4
            
        print('')
                
    print('All tests passed!')
        
def main():
    args = parse_args()
    print('Testing for ...')
    # show the arguments
    print(args)
    test_against_multisearch(args.multisearch_file, args.by_index_file, args.genome_names)
    
if __name__ == '__main__':
    main()