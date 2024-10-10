#include <iostream>
#include <list>
#include <unordered_map>
#include <vector>
#include <queue>
#include <algorithm>
#include <unordered_set>
#include <fstream>

#include "json.hpp"

#include <zlib.h>



using json = nlohmann::json;

using namespace std;

typedef unsigned long long int hash_t;




unordered_map< pair<int, int>, int > computeIntersectionMatrix(vector<vector<hash_t>> sketches) {
    
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

    unordered_map< pair<int, int>, int > pair_to_intersection_count;

    for (auto it = all_hashes_to_sketch_idices.begin(); it != all_hashes_to_sketch_idices.end(); it++) {
        vector<int> sketch_indices = it->second;
        for (int i = 0; i < sketch_indices.size(); i++) {
            auto pair = make_pair(sketch_indices[i], sketch_indices[i]);
            if (pair_to_intersection_count.find(pair) == pair_to_intersection_count.end()) {
                pair_to_intersection_count[pair] = 0;
            }
            pair_to_intersection_count[pair]++;
            for (int j = i + 1; j < sketch_indices.size(); j++) {
                auto pair = make_pair(sketch_indices[i], sketch_indices[j]);
                if (pair_to_intersection_count.find(pair) == pair_to_intersection_count.end()) {
                    pair_to_intersection_count[pair] = 0;
                }
                pair_to_intersection_count[pair]++;
                pair = make_pair(sketch_indices[j], sketch_indices[i]);
                if (pair_to_intersection_count.find(pair) == pair_to_intersection_count.end()) {
                    pair_to_intersection_count[pair] = 0;
                }
                pair_to_intersection_count[pair]++;
            }
        }
    }

    auto end2 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds2 = end2-end;
    std::cout << "Time taken to build the intersection matrix: " << elapsed_seconds2.count() << std::endl;

    return pair_to_intersection_count;

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



unordered_map< pair<int, int>, double > compute_jaccard(unordered_map< pair<int, int>, int > pair_to_intersection) {
    unordered_map< pair<int, int>, double > pair_to_jaccard;
    for (auto it = pair_to_intersection.begin(); it != pair_to_intersection.end(); it++) {
        pair<int, int> pair = it->first;
        int intersection = it->second;

        auto size1 = pair_to_intersection[ make_pair(pair.first, pair.first) ];
        auto size2 = pair_to_intersection[ make_pair(pair.second, pair.second) ];

        int union_size = size1 + size2 - intersection;
        double jaccard = (double)intersection / union_size;
        pair_to_jaccard[pair] = jaccard;
    }
    return pair_to_jaccard;
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


    // create the intersection matrix
    unordered_map< pair<int, int>, int > pair_to_intersection = computeIntersectionMatrix(sketches);

    // create the jaccard matrix
    unordered_map< pair<int, int>, double > pair_to_jaccard = compute_jaccard(pair_to_intersection);

    // write the jaccard matrix to the output file, only write the pairs with jaccard similarity > 0.1
    std::ofstream outputFile(argv[2]);
    for (auto it = pair_to_jaccard.begin(); it != pair_to_jaccard.end(); it++) {
        pair<int, int> pair = it->first;
        double jaccard = it->second;
        if (jaccard > 0.1) {
            outputFile << pair.first << " " << pair.second << " " << jaccard << std::endl;
        }
    }

    // close the output file
    outputFile.close();

    // show the first 10x10 elements in the intersection matrix
    std::cout << "First 10x10 elements of the intersection list: " << std::endl;
    for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
            auto pair = make_pair(i, j);
            if (pair_to_intersection.find(pair) != pair_to_intersection.end()) {
                std::cout << pair_to_intersection[pair] << " ";
            } else {
                std::cout << "0 ";
            }
        }
        std::cout << std::endl;
    }

    auto end = std::chrono::high_resolution_clock::now();
    // show time takes for processing only
    std::cout << "Time taken for processing: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - end_read).count() << " milliseconds" << std::endl;
    std::cout << "Time taken overall: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    
    return 0;

}