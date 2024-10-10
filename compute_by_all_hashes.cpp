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




vector< unordered_map<int, int> > computeIntersectionMatrix(vector<vector<hash_t>> sketches) {
    
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


    vector< unordered_map<int, int> > intersection_vector_info(sketches.size(), unordered_map<int, int>());


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

    auto end2 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds2 = end2-end;
    std::cout << "Time taken to build the intersection matrix: " << elapsed_seconds2.count() << std::endl;

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



std::vector<std::vector<double>> compute_jaccard(std::vector< unordered_map<int, int> > &intersectionMatrix) {
    std::vector<std::vector<double>> jaccardMatrix;
    for (int i = 0; i < intersectionMatrix.size(); i++) {
        jaccardMatrix.push_back(std::vector<double>(intersectionMatrix.size(), 0.0));
    }

    for (int i = 0; i < intersectionMatrix.size(); i++) {
        for (auto it = intersectionMatrix[i].begin(); it != intersectionMatrix[i].end(); it++) {
            int j = it->first;
            int intersection = it->second;
            int union_size = intersectionMatrix[i][i] + intersectionMatrix[j][j] - intersection;
            double jaccard = (double)intersection / (double)union_size;
            jaccardMatrix[i][j] = jaccard;
        }
    }

    return jaccardMatrix;
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
    std::vector< unordered_map<int, int> > intersection_info = computeIntersectionMatrix(sketches);

    // create the jaccard matrix
    std::vector<std::vector<double>> jaccardMatrix = compute_jaccard(intersection_info);

    // show first 10x10 elements of the jaccard matrix
    std::cout << "First 10x10 elements of the jaccard matrix: " << std::endl;
    int smaller = std::min(10, (int)jaccardMatrix.size());
    for (int i = 0; i < smaller; i++) {
        for (int j = 0; j < smaller; j++) {
            std::cout << jaccardMatrix[i][j] << " ";
        }
        std::cout << std::endl;
    }

    // write the jaccard matrix to the output file, only write the pairs with jaccard similarity > 0.1
    std::ofstream outputFile(argv[2]);
    for (int i = 0; i < jaccardMatrix.size(); i++) {
        for (int j = 0; j < jaccardMatrix[i].size(); j++) {
            if (i == j) {
                continue;
            }
            if (jaccardMatrix[i][j] < 0.1) {
                continue;
            }
            outputFile << i << " " << j << " " << jaccardMatrix[i][j] << endl;
        }
    }

    // close the output file
    outputFile.close();

    // show the first 10x10 elements in the intersection matrix
    std::cout << "First 10x10 elements of the intersection matrix: " << std::endl;
    smaller = std::min(10, (int)intersectionMatrix.size());
    for (int i = 0; i < smaller; i++) {
        for (int j = 0; j < smaller; j++) {
            std::cout << intersectionMatrix[i][j] << " ";
        }
        std::cout << std::endl;
    }

    auto end = std::chrono::high_resolution_clock::now();
    // show time takes for processing only
    std::cout << "Time taken for processing: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - end_read).count() << " milliseconds" << std::endl;
    std::cout << "Time taken overall: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    
    return 0;

}