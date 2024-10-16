import sys 
import pandas as pd
from sourmash import load_one_signature


def read_outputs_to_test(output_file_to_test):
    # this file has three columns, separated by a space
    # first two columns: file index in the file list, and third column: jaccard among the two files
    # 45 87 0.88 means the jaccard similarity between the 46th and 88th files is 0.88

    # read the output file to be tested using pandas
    df = pd.read_csv(output_file_to_test, sep=' ', header=None)
    df.columns = ['file1_index', 'file2_index', 'jaccard']
    return df

def read_filelist(file_list):
    # read the file list
    with open(file_list, 'r') as f:
        files = f.readlines()
    return files


if __name__ == '__main__':
    # command line arguments: a file list, and a file to be tested
    if len(sys.argv) < 3:
        print('Usage: python test_outputs.py <file_list> <output_file_to_test>')
        sys.exit(1)

    # read the file list
    file_list = sys.argv[1]
    files = read_filelist(file_list)

    # read the output file to be tested
    output_file_to_test = sys.argv[2]
    df = read_outputs_to_test(output_file_to_test)

    # iterate over the rows of the output file to be tested
    for i, row in df.iterrows():
        file1_index = int(row['file1_index'])
        file2_index = int(row['file2_index'])
        jaccard = row['jaccard']

        # get the filenames from the file list
        filename1 = files[file1_index].strip()
        filename2 = files[file2_index].strip()

        # load the signatures
        sig1 = load_one_signature(filename1)
        sig2 = load_one_signature(filename2)

        jaccard_calculated = sig1.jaccard(sig2)
        # assert that the diff of jaccards is less than 0.001
        print(f'{file1_index} {file2_index} {jaccard_calculated} {jaccard}')
        assert abs(jaccard_calculated - jaccard) <= 0.001

    print('All tests passed!')
