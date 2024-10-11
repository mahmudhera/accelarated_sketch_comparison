#include <iostream>
#include <list>
#include <unordered_map>
#include <vector>
#include <queue>
#include <algorithm>
#include <unordered_set>
#include <fstream>
#include <thread>
#include <cassert>

#include "json.hpp"

#include <zlib.h>



using json = nlohmann::json;

using namespace std;

typedef unsigned long long int hash_t;


std::vector<std::string> sketch_names;
vector<vector<hash_t>> sketches;
int num_sketches;
int num_threads = 1;
unordered_map<hash_t, vector<int>> hash_index;
int ** intersectionMatrix;
int chunk_every_pass;


// create an index using the sketches from start_index to end_index-1
void compute_index_from_sketches(int start_index, int end_index) {
    // assert the start and end indices
    assert(start_index >= 0 && start_index < num_sketches);
    assert(end_index > start_index && end_index <= num_sketches);

    for (int i = start_index; i < end_index; i++) {
        for (int j = start_index; j < end_index; j++) {
            hash_t hash = sketches[i][j];
            if (hash_index.find(hash) == hash_index.end()) {
                hash_index[hash] = vector<int>();
            }
            hash_index[hash].push_back(i);
        }
    }
}


void clear_index() {
    hash_index.clear();
}


// query the sketches from start_index to end_index-1 against the pre-built hash_index
void compute_intersection_matrix_by_sketches(int sketch_start_index, int sketch_end_index, string output_filename) {
    for (int i = sketch_start_index; i < sketch_end_index; i++) {
        for (int j = 0; j < sketches[i].size(); j++) {
            hash_t hash = sketches[i][j];
            if (hash_index.find(hash) != hash_index.end()) {
                vector<int> sketch_indices = hash_index[hash];
                for (int k = 0; k < sketch_indices.size(); k++) {
                    intersectionMatrix[i%chunk_every_pass][sketch_indices[k]%chunk_every_pass]++;
                }
            }
        }
    }

    // write the similarity values to file
    ofstream outfile(output_filename);

    for (int i = sketch_start_index; i < sketch_end_index; i++) {
        for (int j = 0; j < num_sketches; j++) {
            if (i == j) {
                continue;
            }
            if (intersectionMatrix[i%chunk_every_pass][j%chunk_every_pass] == 0) {
                continue;
            }
            float jaccard = 1.0 * intersectionMatrix[i%chunk_every_pass][j%chunk_every_pass] / ( sketches[i].size() + sketches[j].size() - intersectionMatrix[i%chunk_every_pass][j%chunk_every_pass] );
            if (jaccard < 0.1) {
                continue;
            }
            outfile << i << " " << j << " " << jaccard << endl;
        }
    }

    outfile.close();
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


std::vector<hash_t> read_min_hashes(const std::string& json_filename) {
    // if filename contains gz
    if (json_filename.find(".gz") != std::string::npos) {
        auto jsonData = json::parse(decompressGzip(json_filename));
        std::vector<hash_t> min_hashes = jsonData[0]["signatures"][0]["mins"];
        return min_hashes;
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

    // Close the file
    inputFile.close();

    return min_hashes;
}


void read_sketches_one_chunk(int start_index, int end_index) {
    for (int i = start_index; i < end_index; i++) {
        sketches[i] = read_min_hashes(sketch_names[i]);
    }
}


void read_sketches() {
    for (int i = 0; i < num_sketches; i++) {
        sketches.push_back( vector<hash_t>() );
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
}


void get_sketch_names(const std::string& filelist) {
    // the filelist is a file, where each line is a path to a sketch file
    std::ifstream file(filelist);
    std::string line;
    while (std::getline(file, line)) {
        sketch_names.push_back(line);
    }
    num_sketches = sketch_names.size();
}


void show_space_usage() {
    // show the total space used by intersection matrix, the index, the sketches, sketch-names, in MB, all separately
    cout << "-----------------" << endl;
    size_t total_space = 0;
    for (int i = 0; i < num_sketches; i++) {
        total_space += sizeof(int) * num_sketches;
    }
    std::cout << "Total space used by intersection matrix: " << total_space / (1024 * 1024) << " MB" << std::endl;

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


int main(int argc, char* argv[]) {
    
    // command line arguments: filelist outputfile
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <file_list> <out_dir> <num_threads> <num_passes>" << std::endl;
        return 1;
    }

    auto start_program = std::chrono::high_resolution_clock::now();

    num_threads = std::stoi(argv[3]);
    int num_passes = std::stoi(argv[4]);

    // get the sketch names
    get_sketch_names(argv[1]);

    // read the sketches
    read_sketches();
    auto end_read = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to read the sketches: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_read - start_program).count() << " milliseconds" << std::endl;

    // allocate memory for the intersection matrix
    auto dim = num_sketches/num_passes;
    chunk_every_pass = num_sketches / num_passes;
    intersectionMatrix = new int*[dim];
    for (int i = 0; i < num_sketches; i++) {
        intersectionMatrix[i] = new int[dim];
        // memset to zeros
        memset(intersectionMatrix[i], 0, sizeof(int) * dim);
    }

    for (int x = 0; x < num_passes; x++) {
        // clear the index
        clear_index();

        // create the index for sketches from x * num_sketches / num_passes to (x+1) * num_sketches / num_passes
        auto chunk_size_for_index = num_sketches / num_passes;
        auto start_index_for_index = x * chunk_size_for_index;
        auto end_index_for_index = (x == num_passes - 1) ? num_sketches : (x + 1) * chunk_size_for_index;
        compute_index_from_sketches(start_index_for_index, end_index_for_index);

        string dir_name(argv[2]);
        string output_prefix = dir_name + "/pass_" + to_string(x);

        for (int y = 0; y < num_passes; y++) {

            cout << "Pass: " << x << " Iteration: " << y << endl;

            // in this iteration, find the intersection matrix for sketches from y * num_sketches / num_passes to (y+1) * num_sketches / num_passes
            auto chunk_size_for_intersection = num_sketches / num_passes;
            auto start_index_for_intersection = y * chunk_size_for_intersection;
            auto end_index_for_intersection = (y == num_passes - 1) ? num_sketches : (y + 1) * chunk_size_for_intersection;

            vector<thread> threads;

            // compute the intersection matrix using the function compute_intersection_matrix_by_sketches using threads
            for (int i = 0; i < num_threads; i++) {
                auto chunk_size_this_thread = chunk_size_for_intersection / num_threads;
                int start_index = start_index_for_intersection + i * chunk_size_this_thread;
                int end_index = (i == num_threads - 1) ? end_index_for_intersection : start_index_for_intersection + (i + 1) * chunk_size_this_thread;
                string output_filename = output_prefix + "_iter_" + to_string(y) + "_thread_" + to_string(i);
                threads.push_back(thread(compute_intersection_matrix_by_sketches, start_index, end_index, output_filename));
            }

            for (int i = 0; i < num_threads; i++) {
                threads[i].join();
            }

        }
    }

    // show time takes for processing only
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken for processing: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - end_read).count() << " milliseconds" << std::endl;
    std::cout << "Time taken overall: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start_program).count() << " milliseconds" << std::endl;

    // free memory of intersection matrix
    for (int i = 0; i < dim; i++) {
        delete[] intersectionMatrix[i];
    }
    delete[] intersectionMatrix;

    show_space_usage();
    
    return 0;

}


// b57d7400cabd71ad74b8bc62bae70c05eb767a0c : works okay, but projected memory usage: 9TB