#include <iostream>
#include <list>
#include <unordered_map>
#include <vector>
#include <queue>
#include <algorithm>
#include <unordered_set>
#include <fstream>
#include <string>
#include <sstream>
#include <chrono>
#include <stdexcept>
#include <cmath>
#include <thread>

#include "json.hpp"

#include <zlib.h>



using json = nlohmann::json;

using namespace std;

typedef unsigned long long int hash_t;


unordered_map<hash_t, vector<int>> build_index_from_sketches(vector<vector<hash_t>> sketches) {
    auto start = std::chrono::high_resolution_clock::now();

    unordered_map<hash_t, vector<int>> all_hashes_to_sketch_idices;

    for (int i = 0; i < sketches.size(); i++) {
        for (int j = 0; j < sketches[i].size(); j++) {
            hash_t hash = sketches[i][j];
            if (all_hashes_to_sketch_idices.find(hash) == all_hashes_to_sketch_idices.end()) {
                all_hashes_to_sketch_idices[hash] = vector<int>();
            }
            all_hashes_to_sketch_idices[hash].push_back(i);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end-start;
    std::cout << "Time taken to build the hash map: " << elapsed_seconds.count() << std::endl;

    return all_hashes_to_sketch_idices;
}


using MapType = unordered_map<hash_t, std::vector<int>>;

void compute_intersection_one_chunk(MapType::iterator start, MapType::iterator end, int num_sketches, int id) {
    vector< unordered_map<int, int> > intersection_vector_info(num_sketches, unordered_map<int, int>());

    for (auto it = start; it != end; it++) {
        vector<int> sketch_indices = it->second;
        for (int i = 0; i < sketch_indices.size(); i++) {
            if (intersection_vector_info[sketch_indices[i]].find(sketch_indices[i]) == intersection_vector_info[sketch_indices[i]].end()) {
                intersection_vector_info[sketch_indices[i]][sketch_indices[i]] = 1;
            } else {
                intersection_vector_info[sketch_indices[i]][sketch_indices[i]]++;
            }
            for (int j = i + 1; j < sketch_indices.size(); j++) {
                if (intersection_vector_info[sketch_indices[i]].find(sketch_indices[j]) == intersection_vector_info[sketch_indices[i]].end()) {
                    intersection_vector_info[sketch_indices[i]][sketch_indices[j]] = 1;
                } else {
                    intersection_vector_info[sketch_indices[i]][sketch_indices[j]]++;
                }
                if (intersection_vector_info[sketch_indices[j]].find(sketch_indices[i]) == intersection_vector_info[sketch_indices[j]].end()) {
                    intersection_vector_info[sketch_indices[j]][sketch_indices[i]] = 1;
                } else {
                    intersection_vector_info[sketch_indices[j]][sketch_indices[i]]++;
                }
            }
        }
    }

    // create a random file, and write the intersection_vector_info to it
    string random_string = "chunk_" + to_string(id);
    ofstream random_file(random_string);
    for (int i = 0; i < intersection_vector_info.size(); i++) {
        for (auto it = intersection_vector_info[i].begin(); it != intersection_vector_info[i].end(); it++) {
            random_file << i << " " << it->first << " " << it->second << endl;
        }
    }

}



vector< unordered_map<int, int> > computeIntersection(unordered_map<hash_t, vector<int>> all_hashes_to_sketch_idices, int num_sketches) {

    vector< unordered_map<int, int> > intersection_vector_info(num_sketches, unordered_map<int, int>());

    for (auto it = all_hashes_to_sketch_idices.begin(); it != all_hashes_to_sketch_idices.end(); it++) {
        vector<int> sketch_indices = it->second;
        for (int i = 0; i < sketch_indices.size(); i++) {
            if (intersection_vector_info[sketch_indices[i]].find(sketch_indices[i]) == intersection_vector_info[sketch_indices[i]].end()) {
                intersection_vector_info[sketch_indices[i]][sketch_indices[i]] = 1;
            } else {
                intersection_vector_info[sketch_indices[i]][sketch_indices[i]]++;
            }
            for (int j = i + 1; j < sketch_indices.size(); j++) {
                if (intersection_vector_info[sketch_indices[i]].find(sketch_indices[j]) == intersection_vector_info[sketch_indices[i]].end()) {
                    intersection_vector_info[sketch_indices[i]][sketch_indices[j]] = 1;
                } else {
                    intersection_vector_info[sketch_indices[i]][sketch_indices[j]]++;
                }
                if (intersection_vector_info[sketch_indices[j]].find(sketch_indices[i]) == intersection_vector_info[sketch_indices[j]].end()) {
                    intersection_vector_info[sketch_indices[j]][sketch_indices[i]] = 1;
                } else {
                    intersection_vector_info[sketch_indices[j]][sketch_indices[i]]++;
                }
            }
        }
    }

    return intersection_vector_info;

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



std::vector<std::vector<hash_t>> read_sketches(std::vector<std::string> sketch_names) {
    std::vector<std::vector<hash_t>> sketches;
    for (int i = 0; i < sketch_names.size(); i++) {
        if (i % 1000 == 0) {
            std::cout << "Reading sketch " << i << std::endl;
        }
        std::string sketch_name = sketch_names[i];
        std::vector<hash_t> min_hashes = read_min_hashes(sketch_name);
        sketches.push_back(min_hashes);
    }
    std::cout << "Finished reading " << sketch_names.size() << " sketches" << std::endl;
    return sketches;
}





std::vector<std::string> get_sketch_names(const std::string& filelist) {
    // the filelist is a file, where each line is a path to a sketch file
    std::ifstream file(filelist);
    std::vector<std::string> sketch_names;
    std::string line;
    while (std::getline(file, line)) {
        sketch_names.push_back(line);
    }
    return sketch_names;
}



vector<unordered_map<int, float>> compute_jaccard(std::vector< unordered_map<int, int> > &intersection_info) {
    vector<unordered_map<int, float>> jaccard_values = vector<unordered_map<int, float>>();
    for (int i = 0; i < intersection_info.size(); i++) {
        jaccard_values.push_back(unordered_map<int, float>());
    }

    for (int i = 0; i < intersection_info.size(); i++) {
        for (auto it = intersection_info[i].begin(); it != intersection_info[i].end(); it++) {
            int j = it->first;
            int intersection = it->second;
            int union_size = intersection_info[i][i] + intersection_info[j][j] - intersection;
            double jaccard = (double)intersection / (double)union_size;
            jaccard_values[i][j] = jaccard;
        }
    }

    return jaccard_values;
}



int main(int argc, char* argv[]) {
    
    // command line arguments: filelist outputfile
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " filelist outputfile" << std::endl;
        return 1;
    }

    auto start = std::chrono::high_resolution_clock::now();

    // get the sketch names
    std::vector<std::string> sketch_names = get_sketch_names(argv[1]);

    // read the sketches
    std::vector<std::vector<hash_t>> sketches = read_sketches(sketch_names);

    auto end_read = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to read the sketches: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_read - start).count() << " milliseconds" << std::endl;

    // build the index from the sketches
    unordered_map<hash_t, vector<int>> all_hashes_to_sketch_indices = build_index_from_sketches(sketches);

    auto now = std::chrono::high_resolution_clock::now();

    // remove all files with name chunk_*
    system("rm chunk_*");

    // separate the index into 64 parts, each will be processed by a thread
    int num_threads = 64;
    size_t chunk_size = all_hashes_to_sketch_indices.size() / num_threads;
    std::vector<std::thread> threads;
    auto it = all_hashes_to_sketch_indices.begin();

    for (int i = 0; i < num_threads; ++i) {
        auto start_it = it;
        std::advance(it, chunk_size);
        if (i == num_threads - 1) {
            it = all_hashes_to_sketch_indices.end();
        }
        threads.emplace_back(compute_intersection_one_chunk, start_it, it);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // now combine the results from the threads. filenames: chunk_0, chunk_1, ..., chunk_63
    vector<unordered_map<int, int>> intersection_info = vector<unordered_map<int, int>>(sketches.size(), unordered_map<int, int>());

    for (int i = 0; i < num_threads; i++) {
        string filename = "chunk_" + to_string(i);
        ifstream file(filename);
        int sketch_index1, sketch_index2, intersection;
        while (file >> sketch_index1 >> sketch_index2 >> intersection) {
            if (intersection_info[sketch_index1].find(sketch_index2) == intersection_info[sketch_index1].end()) {
                intersection_info[sketch_index1][sketch_index2] = intersection;
            } else {
                intersection_info[sketch_index1][sketch_index2] += intersection;
            }
        }
        file.close();
    }

    // remove all files with name chunk_*
    system("rm chunk_*");

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to compute the intersection: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - now).count() << " milliseconds" << std::endl;

    // create the jaccard matrix
    vector<unordered_map<int, float>> jaccard_values = compute_jaccard(intersection_info);

    // write the jaccard matrix to the output file, only write the pairs with jaccard similarity > 0.1
    std::ofstream outputFile(argv[2]);
    for (int i = 0; i < jaccard_values.size(); i++) {
        for (auto it = jaccard_values[i].begin(); it != jaccard_values[i].end(); it++) {
            if (it->second > 0.1) {
                outputFile << i << " " << it->first << " " << it->second << std::endl;
            }
        }
    }

    // close the output file
    outputFile.close();

    auto end = std::chrono::high_resolution_clock::now();
    // show time takes for processing only
    std::cout << "Time taken for processing: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - end_read).count() << " milliseconds" << std::endl;
    std::cout << "Time taken overall: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    
    return 0;

}



// fcde5c8edc3f5e3dd77184bc5b8841230c8ce534: very fast, but memory usage is high