#include <iostream>
#include <list>
#include <unordered_map>
#include <vector>
#include <queue>
#include <algorithm>
#include <unordered_set>

class HashValueMembersOf {
public:
    unsigned long long int key; // Key member variable
    std::list<unsigned long int> members_of; // List of unsigned long integers

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
        std::list<unsigned long int> list_ids_where_this_element_a_member = minElement->members_of;
        
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



int main() {
    std::vector<std::vector<unsigned long long int>> inputLists = {
        {1, 2, 3, 4, 5},
        {2, 3, 4, 5, 6},
        {3, 4, 5, 6, 7},
        {4, 5, 6, 7, 8},
        {5, 6, 7, 8, 9}
    };

    // print the input lists
    std::cout << "Input lists: " << std::endl;
    for (int i = 0; i < inputLists.size(); i++) {
        for (int j = 0; j < inputLists[i].size(); j++) {
            std::cout << inputLists[i][j] << " ";
        }
        std::cout << std::endl;
    }

    std::vector<std::vector<bool>> bitRepresentation = createBitRepresentation(inputLists);

    std::cout << "Bit representation: " << std::endl;

    for (int i = 0; i < bitRepresentation.size(); i++) {
        for (int j = 0; j < bitRepresentation[i].size(); j++) {
            std::cout << bitRepresentation[i][j] << " ";
        }
        std::cout << std::endl;
    }


    // create a 1000 lists of 10000-20000 elements randomly, element values in the range 0 to 2^32-1
    std::vector<std::vector<unsigned long long int>> randomInputLists;
    for (int i = 0; i < 1000; i++) {
        std::vector<unsigned long long int> randomList;
        int num_elements = 10000 + rand() % 10000;
        for (int j = 0; j < num_elements; j++) {
            randomList.push_back(rand() % 4294967296);
        }
        // sort the list
        std::sort(randomList.begin(), randomList.end());
        randomInputLists.push_back(randomList);
    }

    auto start1 = std::chrono::high_resolution_clock::now();

    // compute the bit representation using the function
    std::vector<std::vector<bool>> randomBitRepresentationUsingFunction = createBitRepresentation(randomInputLists);

    auto end1 = std::chrono::high_resolution_clock::now();

    // compute the bit representation using brute force
    std::vector<std::vector<bool>> randomBitRepresentation;
    for (int i = 0; i < randomInputLists.size(); i++) {
        randomBitRepresentation.push_back(std::vector<bool>());
    }

    auto start = std::chrono::high_resolution_clock::now();

    // create a set for the union of all the elements
    std::unordered_set<unsigned long long int> all_elements;
    for (int i = 0; i < randomInputLists.size(); i++) {
        for (int j = 0; j < randomInputLists[i].size(); j++) {
            all_elements.insert(randomInputLists[i][j]);
        }
    }

    // create an array of the union of all the elements
    std::vector<unsigned long long int> all_elements_array(all_elements.begin(), all_elements.end());

    // show the number of elements in the union
    std::cout << "Number of elements in the union: " << all_elements_array.size() << std::endl;

    // sort the array
    std::sort(all_elements_array.begin(), all_elements_array.end());

    // for each list, create a bit representation
    for (int i = 0; i < randomInputLists.size(); i++) {
        int index = 0;
        for (int j = 0; j < all_elements_array.size(); j++) {
            if (index < randomInputLists[i].size() && randomInputLists[i][index] == all_elements_array[j]) {
                randomBitRepresentation[i].push_back(1);
                index++;
            } else {
                randomBitRepresentation[i].push_back(0);
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();

    

    // compare the two bit representations
    bool areEqual = true;
    for (int i = 0; i < randomBitRepresentation.size(); i++) {
        for (int j = 0; j < randomBitRepresentation[i].size(); j++) {
            if (randomBitRepresentation[i][j] != randomBitRepresentationUsingFunction[i][j]) {
                areEqual = false;
                break;
            }
        }
        if (!areEqual) {
            break;
        }
    }

    if (areEqual) {
        std::cout << "The bit representations are equal" << std::endl;
    } else {
        std::cout << "The bit representations are not equal" << std::endl;
    }

    std::cout << "Time taken by the function: " << std::chrono::duration_cast<std::chrono::milliseconds>(end1 - start1).count() << " milliseconds" << std::endl;

    std::cout << "Time taken by brute force: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;


    return 0;
}
