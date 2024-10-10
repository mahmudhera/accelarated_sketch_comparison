#include <iostream>
#include <list>
#include <unordered_map>
#include <vector>
#include <queue>
#include <algorithm>
#include <unordered_set>
#include <fstream>
#include <thread>

#include "json.hpp"

#include <zlib.h>



using json = nlohmann::json;

using namespace std;

typedef unsigned long long int hash_t;


std::vector<std::string> sketch_names;
vector<vector<hash_t>> sketches;
int num_sketches;
int num_threads = 1;
unordered_map<hash_t, vector<int>> index;
vector<vector<int>> intersectionMatrix;
std::vector<std::vector<double>> jaccardMatrix;



void compute_index_from_sketches() {
    for (int i = 0; i < sketches.size(); i++) {
        for (int j = 0; j < sketches[i].size(); j++) {
            hash_t hash = sketches[i][j];
            if (index.find(hash) == index.end()) {
                index[hash] = vector<int>();
            }
            index[hash].push_back(i);
        }
    }
}



void computeIntersectionMatrix() {

    intersectionMatrix = vector<vector<int>>(num_sketches, vector<int>(num_sketches, 0));

    for (auto it = index.begin(); it != index.end(); it++) {
        vector<int> sketch_indices = it->second;
        for (int i = 0; i < sketch_indices.size(); i++) {
            intersectionMatrix[sketch_indices[i]][sketch_indices[i]]++;
            for (int j = i + 1; j < sketch_indices.size(); j++) {
                intersectionMatrix[sketch_indices[i]][sketch_indices[j]]++;
                intersectionMatrix[sketch_indices[j]][sketch_indices[i]]++;
            }
        }
    }

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



void compute_jaccard() {
    for (int i = 0; i < intersectionMatrix.size(); i++) {
        jaccardMatrix.push_back(std::vector<double>(intersectionMatrix.size(), 0.0));
    }

    for (int i = 0; i < intersectionMatrix.size(); i++) {
        for (int j = 0; j < intersectionMatrix.size(); j++) {
            if (i == j) {
                jaccardMatrix[i][j] = 1.0;
                continue;
            }
            if ((intersectionMatrix[i][i] + intersectionMatrix[j][j] - intersectionMatrix[i][j]) == 0) {
                jaccardMatrix[i][j] = 0.0;
                continue;
            } 
            jaccardMatrix[i][j] = 1.0 * intersectionMatrix[i][j] / (intersectionMatrix[i][i] + intersectionMatrix[j][j] - intersectionMatrix[i][j]);
        }
    }

}



int main(int argc, char* argv[]) {
    
    // command line arguments: filelist outputfile
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " filelist outputfile num_threads" << std::endl;
        return 1;
    }

    auto start_program = std::chrono::high_resolution_clock::now();

    num_threads = std::stoi(argv[3]);

    // get the sketch names
    get_sketch_names(argv[1]);

    // read the sketches
    read_sketches();
    auto end_read = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to read the sketches: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_read - start_program).count() << " milliseconds" << std::endl;

    // create the index
    auto start = std::chrono::high_resolution_clock::now();
    compute_index_from_sketches();
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to create the index: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    // create the intersection matrix
    start = std::chrono::high_resolution_clock::now();
    computeIntersectionMatrix();
    end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to create the intersection matrix: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    // create the jaccard matrix
    start = std::chrono::high_resolution_clock::now();
    compute_jaccard();
    end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to create the jaccard matrix: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    // write the jaccard matrix to the output file, only write the pairs with jaccard similarity > 0.1
    start = std::chrono::high_resolution_clock::now();
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
    end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to write the jaccard matrix: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    // show time takes for processing only
    std::cout << "Time taken for processing: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - end_read).count() << " milliseconds" << std::endl;
    std::cout << "Time taken overall: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start_program).count() << " milliseconds" << std::endl;

    
    return 0;

}