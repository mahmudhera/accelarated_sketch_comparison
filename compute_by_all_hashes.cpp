#include <iostream>
#include <list>
#include <unordered_map>
#include <vector>
#include <queue>
#include <algorithm>
#include <unordered_set>
#include <fstream>
#include <thread>
#include <mutex>

#include "json.hpp"

#include <zlib.h>



using json = nlohmann::json;

using namespace std;

typedef unsigned long long int hash_t;


std::vector<std::string> sketch_names;
std::vector<std::string> genome_names;
vector<vector<hash_t>> sketches;
int num_sketches;
int num_threads = 1;
unordered_map<hash_t, vector<int>> hash_index;
int ** intersectionMatrix;
float containment_threshold = 0.01;
int count_empty_sketch = 0;
mutex mutex_count_empty_sketch;

vector<string> written_file_names;
mutex mutex_written_file_names;



void compute_index_from_sketches() {
    
    // create the index using all the hashes
    for (int i = 0; i < sketches.size(); i++) {
        for (int j = 0; j < sketches[i].size(); j++) {
            hash_t hash = sketches[i][j];
            if (hash_index.find(hash) == hash_index.end()) {
                hash_index[hash] = vector<int>();
            }
            hash_index[hash].push_back(i);
        }
    }

    // remove the hashes that only appear in one sketch
    vector<hash_t> hashes_to_remove;
    for (auto it = hash_index.begin(); it != hash_index.end(); it++) {
        if (it->second.size() == 1) {
            hashes_to_remove.push_back(it->first);
        }
    }
    for (int i = 0; i < hashes_to_remove.size(); i++) {
        hash_index.erase(hashes_to_remove[i]);
    }

    // write the hash index to file
    string filename = "hash_index.txt";
    ofstream outfile(filename);
    for (auto it = hash_index.begin(); it != hash_index.end(); it++) {
        outfile << it->first << " ";
        for (int i = 0; i < it->second.size(); i++) {
            outfile << it->second[i] << " ";
        }
        outfile << endl;
    }
    outfile.close();

}


void load_hash_index() {
    string filename = "hash_index.txt";
    ifstream infile(filename);

    if (!infile.is_open()) {
        cout << "Error: cannot open file " << filename << endl;
        exit(1);
    }

    string line;
    hash_t hash;
    
    while ( getline(infile, line) ) {
        istringstream iss(line);
        iss >> hash;
        vector<int> sketch_indices;
        int sketch_index;
        while (iss >> sketch_index) {
            sketch_indices.push_back(sketch_index);
        }
        hash_index[hash] = sketch_indices;
    }

    infile.close();

}


void show_stats_of_hash_index() {
    // show the number of hashes in the index
    cout << "Number of hashes in the index: " << hash_index.size() << endl;

    // show 10 random hashes and their corresponding sketches
    cout << "10 random hashes and their corresponding sketches:" << endl;
    int count = 0;
    for (auto it = hash_index.begin(); it != hash_index.end(); it++) {
        cout << it->first << " ";
        for (int i = 0; i < it->second.size(); i++) {
            cout << it->second[i] << " ";
        }
        cout << endl;
        count++;
        if (count == 10) {
            break;
        }
    }
}


using MapType = unordered_map<hash_t, vector<int>>;


void compute_intersection_matrix_by_sketches(int sketch_start_index, int sketch_end_index, int thread_id, string out_dir, int pass_id, int negative_offset) {
    for (int i = sketch_start_index; i < sketch_end_index; i++) {
        for (int j = 0; j < sketches[i].size(); j++) {
            hash_t hash = sketches[i][j];
            if (hash_index.find(hash) != hash_index.end()) {
                vector<int> sketch_indices = hash_index[hash];
                for (int k = 0; k < sketch_indices.size(); k++) {
                    intersectionMatrix[i-negative_offset][sketch_indices[k]]++;
                }
            }
        }
    }

    // write the similarity values to file. filename: out_dir/passid_threadid.txt, where id is thread id in 3 digits
    string id_in_three_digits_str = to_string(thread_id);
    while (id_in_three_digits_str.size() < 3) {
        id_in_three_digits_str = "0" + id_in_three_digits_str;
    }
    string pass_id_str = to_string(pass_id);
    //if ( pass_id_str.size() == 1 )
    //    pass_id_str = "0" + pass_id_str;
    string filename = out_dir + "/" + pass_id_str + "_" + id_in_three_digits_str + ".txt";
    ofstream outfile(filename);

    for (int i = sketch_start_index; i < sketch_end_index; i++) {
        for (int j = 0; j < num_sketches; j++) {
            // skip obvious cases
            if (i == j) {
                continue;
            }

            // if nothing in the intersection, then skip
            if (intersectionMatrix[i-negative_offset][j] == 0) {
                continue;
            }

            // if either of the sketches is empty, then skip
            if (sketches[i].size() == 0 || sketches[j].size() == 0) {
                continue;
            }

            // if the divisor in the jaccard calculation is 0, then skip
            if (sketches[i].size() + sketches[j].size() - intersectionMatrix[i-negative_offset][j] == 0) {
                continue;
            }

            double jaccard = 1.0 * intersectionMatrix[i-negative_offset][j] / ( sketches[i].size() + sketches[j].size() - intersectionMatrix[i-negative_offset][j] );
            double containment_i_in_j = 1.0 * intersectionMatrix[i-negative_offset][j] / sketches[i].size();
            double containment_j_in_i = 1.0 * intersectionMatrix[i-negative_offset][j] / sketches[j].size();
            
            // containment_i_in_j is the containment of query in target, i is the query
            if (containment_i_in_j < containment_threshold) {
                continue;
            }

            outfile << i << "," << j << "," << jaccard << "," << containment_i_in_j << "," << containment_j_in_i << endl;
        }
    }

    outfile.close();

    // write the filename to the written_file_names
    mutex_written_file_names.lock();
    written_file_names.push_back(filename);
    mutex_written_file_names.unlock();

}


string decompressGzip(const std::string& filename) {
    // Open file
    gzFile file = gzopen(filename.c_str(), "rb");
    if (!file) {
        throw runtime_error("Failed to open gzip file.");
    }

    // Buffer for decompressed data
    const size_t bufferSize = 8192;
    vector<char> buffer(bufferSize);
    string decompressedData;

    int bytesRead;
    while ((bytesRead = gzread(file, buffer.data(), bufferSize)) > 0) {
        decompressedData.append(buffer.data(), bytesRead);
    }

    gzclose(file);
    return decompressedData;
}


pair<std::vector<hash_t>, string> read_min_hashes(const std::string& json_filename) {
    // if filename contains gz
    if (json_filename.find(".gz") != std::string::npos) {
        auto jsonData = json::parse(decompressGzip(json_filename));
        std::vector<hash_t> min_hashes = jsonData[0]["signatures"][0]["mins"];
        std::string genome_name = jsonData[0]["name"];
        return {min_hashes, genome_name};
    }

    // Open the JSON file
    std::ifstream inputFile(json_filename);

    // Check if the file is open
    if (!inputFile.is_open()) {
        std::cerr << "Could not open the file!" << std::endl;
        return {};
    }

    // Parse the JSON data
    json jsonData;
    inputFile >> jsonData;

    // Access and print values
    std::vector<hash_t> min_hashes = jsonData[0]["signatures"][0]["mins"];
    std::string genome_name = jsonData[0]["name"];

    // Close the file
    inputFile.close();

    return {min_hashes, genome_name};
}


void read_sketches_one_chunk(int start_index, int end_index) {
    for (int i = start_index; i < end_index; i++) {
        auto min_hashes_genome_name = read_min_hashes(sketch_names[i]);
        sketches[i] = min_hashes_genome_name.first;
        genome_names[i] = min_hashes_genome_name.second;
        if (sketches[i].size() == 0) {
            mutex_count_empty_sketch.lock();
            count_empty_sketch++;
            mutex_count_empty_sketch.unlock();
        }
    }
}


void read_sketches() {
    for (int i = 0; i < num_sketches; i++) {
        sketches.push_back( vector<hash_t>() );
    }
    // initialize genome_names vector using empty strings
    for (int i = 0; i < num_sketches; i++) {
        genome_names.push_back("");
    }

    int chunk_size = num_sketches / num_threads;
    vector<thread> threads;
    for (int i = 0; i < num_threads; i++) {
        int start_index = i * chunk_size;
        int end_index = (i == num_threads - 1) ? num_sketches : (i + 1) * chunk_size;
        threads.push_back(thread(read_sketches_one_chunk, start_index, end_index));
    }
    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }
    // show the number of empty sketches
    cout << "Number of empty sketches: " << count_empty_sketch << endl;

    // show the ids of these empty sketches
    for (int i = 0; i < num_sketches; i++) {
        if (sketches[i].size() == 0) {
            cout << i << " ";
        }
    }
    cout << endl;
}


void get_sketch_names(const std::string& filelist) {
    // the filelist is a file, where each line is a path to a sketch file
    std::ifstream file(filelist);
    if (!file.is_open()) {
        std::cerr << "Could not open the filelist: " << filelist << std::endl;
        return;
    }
    std::string line;
    while (std::getline(file, line)) {
        sketch_names.push_back(line);
    }
    num_sketches = sketch_names.size();
}


void show_space_usages(int num_passes) {
    // show the total space used by intersection matrix, the index, the sketches, sketch-names, in MB, all separately
    cout << "-----------------" << endl;
    size_t total_space = 0;
    for (int i = 0; i < num_sketches; i++) {
        total_space += sizeof(int) * num_sketches;
    }
    std::cout << "Total space used by intersection matrix: " << total_space / (1024 * 1024 * num_passes) << " MB" << std::endl;

    total_space = 0;
    for (auto it = hash_index.begin(); it != hash_index.end(); it++) {
        total_space += sizeof(hash_t) + sizeof(int) * it->second.size();
    }
    std::cout << "Total space used by the hash-index: " << total_space / (1024 * 1024) << " MB" << std::endl;

    total_space = 0;
    for (int i = 0; i < num_sketches; i++) {
        total_space += sizeof(hash_t) * sketches[i].size();
    }
    std::cout << "Total space used by sketches: " << total_space / (1024 * 1024) << " MB" << std::endl;

    total_space = 0;
    for (int i = 0; i < num_sketches; i++) {
        total_space += sketch_names[i].size();
    }
    std::cout << "Total space used by sketch names: " << total_space / (1024 * 1024) << " MB" << std::endl;
    cout << "-----------------" << endl;
}


void write_all_genome_names() {
    string all_genome_names = "all_genome_names.txt";
    ofstream outfile(all_genome_names);
    for (int i = 0; i < num_sketches; i++) {
        outfile << genome_names[i] << endl;
    }
    outfile.close();
}


void cleanup(int num_sketches_each_pass) {
    // free memory of intersection matrix
    for (int i = 0; i < num_sketches_each_pass + 1; i++) {
        delete[] intersectionMatrix[i];
    }
    delete[] intersectionMatrix;
}


int main(int argc, char* argv[]) {
    
    // command line arguments: filelist outputfile
    if (argc != 8) {
        std::cerr << "Usage: " << argv[0] << " <file_list> <out_dir> <num_threads> <num_passes> <containment_threshold> <test_mode> <load_hash_index>" << std::endl;
        return 1;
    }

    auto start_program = std::chrono::high_resolution_clock::now();

    num_threads = std::stoi(argv[3]);
    int num_passes = std::stoi(argv[4]);
    containment_threshold = std::stof(argv[5]);
    bool test_mode = std::stoi(argv[6]);
    bool load_hash_index_flag = std::stoi(argv[7]);

    // get the sketch names
    cout << "Getting sketch names..." << endl;
    get_sketch_names(argv[1]);

    // read the sketches
    cout << "Reading sketches..." << endl;
    read_sketches();
    auto end_read = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to read the sketches: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_read - start_program).count() << " milliseconds" << std::endl;

    // create the index if needed. otherwise, load from file
    auto start = std::chrono::high_resolution_clock::now();
    if (load_hash_index_flag) {
        cout << "Loading hash index..." << endl;
        load_hash_index();
    } else {
        cout << "Creating hash index..." << endl;
        compute_index_from_sketches();
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to create/load the index: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    // show stats of the hash index
    show_stats_of_hash_index();

    // create the intersection matrix
    start = std::chrono::high_resolution_clock::now();

    // allocate memory for the intersection matrix
    int num_sketches_each_pass = ceil(1.0 * num_sketches / num_passes);
    intersectionMatrix = new int*[num_sketches_each_pass + 1];
    for (int i = 0; i < num_sketches_each_pass + 1; i++) {
        intersectionMatrix[i] = new int[num_sketches];
    }

    // in test mode, exit now
    if (test_mode) {
        cleanup(num_sketches_each_pass);
        show_space_usages(num_passes);
        return 0;
    }
    
    written_file_names = vector<string>();

    for (int pass_id = 0; pass_id < num_passes; pass_id++) {
        // set zeros in the intersection matrix
        for (int i = 0; i < num_sketches_each_pass+1; i++) {
            for (int j = 0; j < num_sketches; j++) {
                intersectionMatrix[i][j] = 0;
            }
        }

        // indices
        int sketch_idx_start_this_pass = pass_id * num_sketches_each_pass;
        int sketch_idx_end_this_pass = (pass_id == num_passes - 1) ? num_sketches : (pass_id + 1) * num_sketches_each_pass;
        int negative_offset = pass_id * num_sketches_each_pass;
        int num_sketches_this_pass = sketch_idx_end_this_pass - sketch_idx_start_this_pass;
        
        // create threads
        vector<thread> threads;
        int chunk_size = num_sketches_this_pass / num_threads;
        for (int i = 0; i < num_threads; i++) {
            int start_index_this_thread = sketch_idx_start_this_pass + i * chunk_size;
            int end_index_this_thread = (i == num_threads - 1) ? sketch_idx_end_this_pass : sketch_idx_start_this_pass + (i + 1) * chunk_size;
            threads.push_back( thread(compute_intersection_matrix_by_sketches, start_index_this_thread, end_index_this_thread, i, argv[2], pass_id, negative_offset) );
        }

        // join threads
        for (int i = 0; i < num_threads; i++) {
            threads[i].join();
        }

        // show progress
        std::cout << "Pass " << pass_id << "/" << num_passes << " done." << std::endl;
    }


    // write all the written file names to a file
    string written_file_names_filename = "by_index_file_names.txt";
    fstream written_filelist_file(written_file_names_filename, ios::out);
    for (int i = 0; i < written_file_names.size(); i++) {
        written_filelist_file << written_file_names[i] << endl;
    }
    written_filelist_file.close();
    
    end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to create the intersection matrix: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    // show time takes for processing only
    std::cout << "Time taken for processing: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - end_read).count() << " milliseconds" << std::endl;
    std::cout << "Time taken overall: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start_program).count() << " milliseconds" << std::endl;

    // cleanup
    cleanup(num_sketches_each_pass);

    // show space usages
    show_space_usages(num_passes);

    // write all genome names
    write_all_genome_names();
    
    return 0;

}


// 22409122b20702fd008413959b7d765e655239e2