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

#include <cuda_runtime.h>
#include <cublas_v2.h>


#define CHECK_CUDA(call) \
    if ((call) != cudaSuccess) { \
        std::cerr << "CUDA error at " << __FILE__ << ":" << __LINE__ << std::endl; \
        exit(EXIT_FAILURE); \
    }

#define CHECK_CUBLAS(call) \
    if ((call) != CUBLAS_STATUS_SUCCESS) { \
        std::cerr << "cuBLAS error at " << __FILE__ << ":" << __LINE__ << std::endl; \
        exit(EXIT_FAILURE); \
    }


using json = nlohmann::json;

using namespace std;

typedef unsigned long long int hash_t;


std::vector<std::string> sketch_names;
std::vector<std::string> genome_names;
vector<vector<hash_t>> sketches;
int num_sketches;
int num_threads = 1;
unordered_map<hash_t, vector<int>> hash_index;
int count_empty_sketch = 0;
mutex mutex_count_empty_sketch;
float **bit_representation;


void cleanup() {
    for (int i = 0; i < hash_index.size(); i++) {
        delete[] bit_representation[i];
    }
    delete[] bit_representation;
}


void compute_bit_representation() {
    // allocate memory for the bit representation
    size_t num_rows = hash_index.size();
    size_t num_cols = sketches.size();
    bit_representation = new float*[num_rows];
    for (int i = 0; i < num_rows; i++) {
        bit_representation[i] = new float[num_cols];
    }

    // set zeros in the bit representation
    for (int i = 0; i < num_rows; i++) {
        // memset to 0
        memset(bit_representation[i], 0, num_cols * sizeof(float));
    }

    // fill the bit representation
    int row_idx = 0;
    for (auto it = hash_index.begin(); it != hash_index.end(); it++) {
        for (int i = 0; i < it->second.size(); i++) {
            int col_idx = it->second[i];
            bit_representation[row_idx][col_idx] = 1.0;
        }
        row_idx++;
    }
}


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


int main(int argc, char* argv[]) {
    
    // command line arguments: filelist outputfile
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <file_list> <out_dir> <num_threads> <gpu_id>" << std::endl;
        return 1;
    }

    auto start_program = std::chrono::high_resolution_clock::now();

    num_threads = std::stoi(argv[3]);

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
    cout << "Creating hash index..." << endl;
    compute_index_from_sketches();
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to create the index: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    // create the intersection matrix
    start = std::chrono::high_resolution_clock::now();

    // compute the bit representation
    compute_bit_representation();

    // set the device using the gpu_id
    CHECK_CUDA(cudaSetDevice(std::stoi(argv[4])));

    // allocate memory on device
    float *d_bit_representation;
    CHECK_CUDA(cudaMalloc(&d_bit_representation, num_sketches * hash_index.size() * sizeof(float)));

    // copy the bit representation to device
    CHECK_CUDA(cudaMemcpy(d_bit_representation, bit_representation[0], num_sketches * hash_index.size() * sizeof(float), cudaMemcpyHostToDevice));

    // create a cublas handle
    cublasHandle_t handle;
    CHECK_CUBLAS(cublasCreate(&handle));

    // create a matrix of size num_rows x num_rows to store the dot products
    float *h_C;
    h_C = new float[num_sketches * num_sketches];
    float *d_C;
    CHECK_CUDA(cudaMalloc(&d_C, num_sketches * num_sketches * sizeof(float)));

    // compute bit_representation * bit_representation^T
    float alpha = 1.0;
    float beta = 0.0;
    CHECK_CUBLAS(cublasSgemm(handle, CUBLAS_OP_N, CUBLAS_OP_T, num_sketches, num_sketches, hash_index.size(), &alpha, d_bit_representation, num_sketches, d_bit_representation, num_sketches, &beta, d_C, num_sketches));

    // copy the result back to host
    CHECK_CUDA(cudaMemcpy(h_C, d_C, num_sketches * num_sketches * sizeof(float), cudaMemcpyDeviceToHost));

    // free CUDA memory
    CHECK_CUDA(cudaFree(d_bit_representation));
    CHECK_CUDA(cudaFree(d_C));

    // destroy the handle
    CHECK_CUBLAS(cublasDestroy(handle));

    // show first 10x10 of the intersection matrix
    for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
            cout << h_C[i * num_sketches + j] << " ";
        }
        cout << endl;
    }

    
    end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to create the intersection matrix: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

    // show time takes for processing only
    std::cout << "Time taken for processing: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - end_read).count() << " milliseconds" << std::endl;
    std::cout << "Time taken overall: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start_program).count() << " milliseconds" << std::endl;

    // cleanup
    cleanup();
    
    return 0;

}