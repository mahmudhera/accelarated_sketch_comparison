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




std::vector<std::vector<unsigned long long int>> computeIntersectionMatrix(const std::vector<std::vector<unsigned long long int>>& inputLists) {
    // create return list: an nxn matrix, where n is the number of input lists
    std::vector<std::vector<unsigned long long int>> intersectionMatrix;
    for (int i = 0; i < inputLists.size(); i++) {
        intersectionMatrix.push_back(std::vector<unsigned long long int>(inputLists.size(), 0));
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
    
    // while the heap is not empty
    while (!minHeap.isEmpty()) {
        // pop the min element from the heap
        HashValueMembersOf* minElement = minHeap.pop();
        
        // get the lists that contain this element using the member_of list
        std::vector<unsigned long int> list_ids_where_this_element_a_member = minElement->members_of;
        
        // for each list that contain this element
        for (unsigned long int i = 0; i < list_ids_where_this_element_a_member.size(); i++) {

            unsigned long int list_id1 = list_ids_where_this_element_a_member[i];

            intersectionMatrix[list_id1][list_id1]++; // increment the intersection matrix for the same list

            // for each other list that contain this element
            for (unsigned long int j = i + 1; j < list_ids_where_this_element_a_member.size(); j++) {

                unsigned long int list_id2 = list_ids_where_this_element_a_member[j];

                intersectionMatrix[list_id1][list_id2]++;
                intersectionMatrix[list_id2][list_id1]++;
            }
            
            // if the index is less than the size of the list, insert the element at that index into the heap
            if (indices[list_id1] < inputLists[list_id1].size()) {
                auto next_element = inputLists[list_id1][indices[list_id1]];
                minHeap.insert(next_element, list_id1);

                // increment the index for that list
                indices[list_id1]++; 

            }

            
        }

        /* FOR DEBUGGING
        // show the element and the intersection matrix for the current iteration
        std::cout << "Element: " << minElement->key << std::endl;
        std::cout << "Lists containing this element: ";
        for (auto list_id : list_ids_where_this_element_a_member) {
            std::cout << list_id << " ";
        }

        std::cout << "Intersection Matrix: " << std::endl;
        for (int i = 0; i < intersectionMatrix.size(); i++) {
            for (int j = 0; j < intersectionMatrix[i].size(); j++) {
                std::cout << intersectionMatrix[i][j] << " ";
            }
            std::cout << std::endl;
        }

        // show the heap
        std::cout << "Heap: " << std::endl;
        minHeap.printHeap();

        // show the indices array
        std::cout << "Indices: ";
        for (auto index : indices) {
            std::cout << index << " ";
        }

        std::cout << "----" << std::endl;
        */
        

        // delete the min element
        delete minElement;

    }

    return intersectionMatrix;

}




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







std::vector<unsigned long long int> read_min_hashes(const std::string& json_filename) {
    // if filename contains gz
    if (json_filename.find(".gz") != std::string::npos) {
        auto jsonData = json::parse(decompressGzip(json_filename));
        std::vector<unsigned long long int> min_hashes = jsonData[0]["signatures"][0]["mins"];
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
    std::vector<unsigned long long int> min_hashes = jsonData[0]["signatures"][0]["mins"];

    // Close the file
    inputFile.close();

    return min_hashes;
}



std::vector<std::vector<unsigned long long int>> read_sketches(std::vector<std::string> sketch_names) {
    std::vector<std::vector<unsigned long long int>> sketches;
    for (int i = 0; i < sketch_names.size(); i++) {
        if (i % 1000 == 0) {
            std::cout << "Reading sketch " << i << std::endl;
        }
        std::string sketch_name = sketch_names[i];
        std::vector<unsigned long long int> min_hashes = read_min_hashes(sketch_name);
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
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " filelist outputfile" << std::endl;
        return 1;
    }

    auto start = std::chrono::high_resolution_clock::now();

    // get the sketch names
    std::vector<std::string> sketch_names = get_sketch_names(argv[1]);

    // read the sketches
    std::vector<std::vector<unsigned long long int>> sketches = read_sketches(sketch_names);

    auto end_read = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to read the sketches: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_read - start).count() << " milliseconds" << std::endl;


    // create the intersection matrix
    std::vector<std::vector<unsigned long long int>> intersectionMatrix = computeIntersectionMatrix(sketches);

    // create the jaccard matrix
    std::vector<std::vector<double>> jaccardMatrix = compute_jaccard(intersectionMatrix);

    // show first 10x10 elements of the jaccard matrix
    std::cout << "First 10x10 elements of the jaccard matrix: " << std::endl;
    int smaller = std::min(10, (int)jaccardMatrix.size());
    for (int i = 0; i < smaller; i++) {
        for (int j = 0; j < smaller; j++) {
            std::cout << jaccardMatrix[i][j] << " ";
        }
        std::cout << std::endl;
    }

    // write the jaccard matrix to the output file
    std::ofstream outputFile(argv[2]);
    for (int i = 0; i < jaccardMatrix.size(); i++) {
        for (int j = 0; j < jaccardMatrix[i].size(); j++) {
            outputFile << jaccardMatrix[i][j] << " ";
        }
        outputFile << std::endl;
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



// corresponding sourmash compare takes 7m 21s
// our code: 204828 milliseconds
