import argparse
import zipfile
import os

def parse_args():
    # arguments: filelist, output_zip_path
    parser = argparse.ArgumentParser(description='Create a zip file from a list of files')
    parser.add_argument('filelist', type=str, help='Path to the file containing the list of files to be zipped')
    parser.add_argument('output_zip_path', type=str, help='Path to the output zip file')
    return parser.parse_args()

def main():
    args = parse_args()
    with open(args.filelist, 'r') as f:
        filelist = f.read().splitlines()

    # structure of the zip file
    # SOURMASH-MANIFEST.csv
    # signatures/
        # signature1.sig
        # signature2.sig
        # ...

    # create a directory working_dir
    working_dir = 'working_dir'
    os.makedirs(working_dir, exist_ok=True)

    # create a directory 'signatures' in working_dir 
    signatures_dir = os.path.join(working_dir, 'signatures')
    os.makedirs(signatures_dir, exist_ok=True)

    # copy the files in filelist to the 'signatures' directory
    print ("copying files to signatures directory")
    for file in filelist:
        os.system(f'cp {file} {signatures_dir}')
    print ("done copying files")

    # gzip all files in the 'signatures' directory
    print ("gzipping files in signatures")
    for file in os.listdir(signatures_dir):
        os.system(f'gzip {os.path.join(signatures_dir, file)}')
    print ("done gzipping files")

    # create a file 'SOURMASH-MANIFEST.csv' in working_dir
    print('creating manifest file')
    cmd = f'sourmash sig manifest -o {working_dir}/SOURMASH-MANIFEST.csv ' + signatures_dir
    os.system(cmd)
    print('done creating manifest file')

    # create a zip file
    print('creating zip file')
    with zipfile.ZipFile(args.output_zip_path, 'w') as zipf:
        for file in os.listdir(working_dir):
            zipf.write(os.path.join(working_dir, file), file)
            if file == 'signatures':
                for sig_file in os.listdir(signatures_dir):
                    zipf.write(os.path.join(signatures_dir, sig_file), os.path.join('signatures', sig_file))
    print('done creating zip file')

    # remove the working directory
    print('removing working directory')
    os.system(f'rm -rf {working_dir}')
    print('done removing working directory')

if __name__ == '__main__':
    main()
        
        