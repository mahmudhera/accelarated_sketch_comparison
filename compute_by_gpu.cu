#include <iostream>
#include <list>
#include <unordered_map>
#include <vector>
#include <queue>
#include <algorithm>
#include <unordered_set>
#include <fstream>

#include "json.hpp"

#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <chrono>

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

class HashValueMembersOf {
public:
    unsigned long long int key; // Key member variable
    std::vector<unsigned long int> members_of; // List of unsigned long integers

    // Constructor to initialize key and optionally initialize the list
    HashValueMembersOf(unsigned long long int k, unsigned long int member) : key(k) {
        members_of.push_back(member);
    }

    // Add a member to the list
    void addMember(unsigned long int member) {
        members_of.push_back(member);
    }

    // Print key and members_of
    void printMembers() const {
        std::cout << "Key: " << key << " Members of: ";
        for (const auto& val : members_of) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    }
};

// Custom comparator to make the priority_queue act as a min-heap
struct Compare {
    bool operator()(const HashValueMembersOf* lhs, const HashValueMembersOf* rhs) const {
        return lhs->key > rhs->key; // Min-heap based on key
    }
};

class MinHeap {
private:
    std::priority_queue<HashValueMembersOf*, std::vector<HashValueMembersOf*>, Compare> heap;
    std::unordered_map<unsigned long long int, HashValueMembersOf*> lookup; // Hash map to keep track of keys

public:
    // Insert function: If the key exists, append to members_of; if not, create a new object
    void insert(unsigned long long int key, unsigned long int member_of) {
        if (lookup.find(key) == lookup.end()) {
            // Key not found, create new MyClass object and insert it into the heap
            HashValueMembersOf* newObject = new HashValueMembersOf(key, member_of);
            heap.push(newObject);
            lookup[key] = newObject; // Track the new object in the map
        } else {
            // Key found, append member_of to the existing object
            lookup[key]->addMember(member_of);
        }
    }

    // Pop the top (min) element from the heap
    HashValueMembersOf* pop() {
        if (heap.empty()) {
            return nullptr;
        }

        HashValueMembersOf* top = heap.top();
        heap.pop();
        lookup.erase(top->key); // Remove from the lookup map
        return top;
    }

    // Check if the heap is empty
    bool isEmpty() const {
        return heap.empty();
    }

    // Print the entire heap (min-heap order cannot be guaranteed)
    void printHeap() {
        std::priority_queue<HashValueMembersOf*, std::vector<HashValueMembersOf*>, Compare> tempHeap = heap;
        
        std::cout << "Heap keys in lookup" << std::endl;
        for (const auto& pair : lookup) {
            std::cout << pair.first << " ";
        }
        std::cout << std::endl;

        std::cout << "Heap contents: " << std::endl;
        while (!tempHeap.empty()) {
            HashValueMembersOf* top = tempHeap.top();
            top->printMembers();
            tempHeap.pop();
        }
    }
};



std::vector<std::vector<bool>> createBitRepresentation(const std::vector<std::vector<unsigned long long int>>& inputLists) {
    // create return list
    std::vector<std::vector<bool>> bitRepresentation;
    for (int i = 0; i < inputLists.size(); i++) {
        bitRepresentation.push_back(std::vector<bool>());
    }

    // create an array of indices to keep track of the current element of each list
    std::vector<int> indices(inputLists.size(), 0);

    // create a minheap to store the first element of all these lists
    MinHeap minHeap;
    for (int i = 0; i < inputLists.size(); i++) {
        if (!inputLists[i].empty()) {
            minHeap.insert(inputLists[i][0], i);
            indices[i] = 1;
        }
    }

    unsigned long long int num_elements_processed = 0;
    // while the heap is not empty
    while (!minHeap.isEmpty()) {
        // pop the min element from the heap
        HashValueMembersOf* minElement = minHeap.pop();
        
        // insert 0 into the bit representation for all the lists
        for (int i = 0; i < inputLists.size(); i++) {
            bitRepresentation[i].push_back(0);
        }
        
        // get the lists that contain this element using the member_of list
        std::vector<unsigned long int> list_ids_where_this_element_a_member = minElement->members_of;
        
        // for each list that contain this element
        for (auto list_id : list_ids_where_this_element_a_member) {
            
            // set the bit at the index to 1
            bitRepresentation[list_id][num_elements_processed] = 1;
            
            // if the index is less than the size of the list, insert the element at that index into the heap
            if (indices[list_id] < inputLists[list_id].size()) {
                auto next_element = inputLists[list_id][indices[list_id]];
                minHeap.insert(next_element, list_id);
            }

            // increment the index for that list
            indices[list_id]++;
        }

        // increment the number of elements processed
        num_elements_processed++;

        // delete the min element
        delete minElement;

    }

    return bitRepresentation;
    
   
}





std::vector<unsigned long long int> read_min_hashes(const std::string& json_filename) {
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
    std::vector<unsigned long long int> min_hashes = jsonData[0]["signatures"][0]["mins"];

    // Close the file
    inputFile.close();

    return min_hashes;
}



std::vector<std::vector<unsigned long long int>> read_sketches(std::vector<std::string> sketch_names) {
    std::vector<std::vector<unsigned long long int>> sketches;
    for (const auto& sketch_name : sketch_names) {
        std::vector<unsigned long long int> min_hashes = read_min_hashes(sketch_name);
        sketches.push_back(min_hashes);
    }
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



std::vector<std::vector<double>> compute_jaccard(std::vector<std::vector<unsigned long long int>> &intersectionMatrix) {
    std::vector<std::vector<double>> jaccardMatrix;
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

    return jaccardMatrix;
}



int main(int argc, char* argv[]) {
    
    // command line arguments: filelist outputfile
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " filelist outputfile gpu_id" << std::endl;
        return 1;
    }

    auto start = std::chrono::high_resolution_clock::now();

    // get the sketch names
    std::vector<std::string> sketch_names = get_sketch_names(argv[1]);

    // read the sketches
    std::vector<std::vector<unsigned long long int>> sketches = read_sketches(sketch_names);

    // compute the bit representation
    std::vector<std::vector<bool>> bitRepresentation = createBitRepresentation(sketches);

    // represent the bit representation as a float matrix
    size_t num_rows = bitRepresentation.size();
    size_t num_cols = bitRepresentation[0].size();

    float *h_A = new float[num_rows * num_cols];
    // store the bit representation in the matrix in column-major order
    for (int i = 0; i < num_rows; i++) {
        for (int j = 0; j < num_cols; j++) {
            h_A[j * num_rows + i] = bitRepresentation[i][j];
        }
    }

    // set the device using the gpu_id
    CHECK_CUDA(cudaSetDevice(std::stoi(argv[3])));

    // allocate memory on the device
    float *d_A;
    CHECK_CUDA(cudaMalloc(&d_A, num_rows * num_cols * sizeof(float)));

    // copy the data to the device
    CHECK_CUDA(cudaMemcpy(d_A, h_A, num_rows * num_cols * sizeof(float), cudaMemcpyHostToDevice));

    // create a cublas handle
    cublasHandle_t handle;
    CHECK_CUBLAS(cublasCreate(&handle));

    // create a matrix of size num_rows x num_rows to store the dot products
    float *h_C;
    h_C = new float[num_rows * num_rows];
    float *d_C;
    CHECK_CUDA(cudaMalloc(&d_C, num_rows * num_rows * sizeof(float)));

    // compute A * A^T
    float alpha = 1.0;
    float beta = 0.0;
    CHECK_CUBLAS(cublasSgemm(handle, CUBLAS_OP_N, CUBLAS_OP_T, num_rows, num_rows, num_cols, &alpha, d_A, num_rows, d_A, num_rows, &beta, d_C, num_rows));

    // copy the result back to the host
    CHECK_CUDA(cudaMemcpy(h_C, d_C, num_rows * num_rows * sizeof(float), cudaMemcpyDeviceToHost));

    // free the memory
    CHECK_CUDA(cudaFree(d_A));
    CHECK_CUDA(cudaFree(d_C));
    
    // destroy the handle
    CHECK_CUBLAS(cublasDestroy(handle));

    // the result h_C is in column-major order, convert it to a 2D float matrix
    std::vector<std::vector<unsigned long long int>> intersectionMatrix;
    for (int i = 0; i < num_rows; i++) {
        intersectionMatrix.push_back(std::vector<unsigned long long int>(num_rows, 0));
    }

    for (int i = 0; i < num_rows; i++) {
        for (int j = 0; j < num_rows; j++) {
            intersectionMatrix[i][j] = h_C[j * num_rows + i];
        }
    }

    // delete the host memory
    delete[] h_A;
    delete[] h_C;

    // compute the jaccard similarity
    std::vector<std::vector<double>> jaccardMatrix = compute_jaccard(intersectionMatrix);

    // write the jaccard matrix to a file
    std::ofstream outputFile(argv[2]);
    for (int i = 0; i < jaccardMatrix.size(); i++) {
        for (int j = 0; j < jaccardMatrix.size(); j++) {
            outputFile << jaccardMatrix[i][j] << " ";
        }
        outputFile << std::endl;
    }
    outputFile.close();

    // show the first 10x10 elements of the jaccard matrix
    for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
            std::cout << jaccardMatrix[i][j] << " ";
        }
        std::cout << std::endl;
    }


    // show the result, first 10x10
    for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
            std::cout << intersectionMatrix[i][j] << " ";
        }
        std::cout << std::endl;
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl; 

    return 0;

}