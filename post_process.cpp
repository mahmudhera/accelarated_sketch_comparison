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
#include <chrono>
#include <string>
#include <sstream>
#include <thread>


using json = nlohmann::json;
using namespace std;

typedef unsigned long long int hash_t;

vector<string> sketch_names;
vector<vector<hash_t>> sketches;
int num_sketches;
int num_threads = 1;
int count_empty_sketch = 0;
mutex mutex_count_empty_sketch;
vector<pair<int, int>> genome_id_size_pairs;
vector<vector<int>> similars;



void read_similar_info_one_file(string filename) {
    
    ifstream simFile(filename);
    if (!simFile.is_open()) {
        cerr << "Could not open the file: " << filename << endl;
        exit(1);
    }
    string simPairLine;
    while (getline(simFile, simPairLine)) {
        // each line looks like: id1,id2,sim1,sim2,sim3, comma separated
        istringstream iss(simPairLine);
        int id1, id2;
        char ch;
        iss >> id1;
        iss >> ch;
        iss >> id2;
        similars[id1].push_back(id2);
    }
    simFile.close();

}



void read_similar_info(string simFileList, int num_threads) {
    for (int i = 0; i < num_sketches; i++) {
        similars.push_back(vector<int>());
    }

    ifstream file(simFileList);
    if (!file.is_open()) {
        cerr << "Could not open the file: " << simFileList << endl;
        return;
    }
    vector<string> filenames;
    string line;
    while (getline(file, line)) {
        filenames.push_back(line);
    }
    file.close();

    // read the similar info, one thread for one file
    int num_processed = 0;
    int to_process = filenames.size();
    while (num_processed < to_process) {
        vector<thread> threads;
        for (int i = 0; i < num_threads; i++) {
            if (num_processed == to_process) {
                break;
            }
            threads.push_back(thread(read_similar_info_one_file, filenames[num_processed]));
            num_processed++;
        }
        for (int i = 0; i < threads.size(); i++) {
            threads[i].join();
        }
    }

}



vector<hash_t> read_min_hashes(const string& json_filename) {
    
    // Open the JSON file
    ifstream inputFile(json_filename);

    // Check if the file is open
    if (!inputFile.is_open()) {
        cerr << "Could not open the file!" << endl;
        return {};
    }

    // Parse the JSON data
    json jsonData;
    inputFile >> jsonData;

    // Access and print values
    vector<hash_t> min_hashes = jsonData[0]["signatures"][0]["mins"];

    // Close the file
    inputFile.close();

    return min_hashes;
}



void read_sketches_one_chunk(int start_index, int end_index) {
    for (int i = start_index; i < end_index; i++) {
        auto min_hashes_genome_name = read_min_hashes(sketch_names[i]);
        sketches[i] = min_hashes_genome_name;
        if (sketches[i].size() == 0) {
            mutex_count_empty_sketch.lock();
            count_empty_sketch++;
            mutex_count_empty_sketch.unlock();
        }
        genome_id_size_pairs[i] = {i, sketches[i].size()};
    }
}




void read_sketches() {
    for (int i = 0; i < num_sketches; i++) {
        sketches.push_back( vector<hash_t>() );
        genome_id_size_pairs.push_back({-1, 0});
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

    // command line arguments: sigFileList simFileList numThreads
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <sigFileList> <simFileList> <numThreads> <outFileName>" << std::endl;
        return 1;
    }

    string sigFileList = argv[1];
    string simFileList = argv[2];
    num_threads = stoi(argv[3]);
    string outFileName = argv[4];

    // read the sketch files
    auto start_program = std::chrono::high_resolution_clock::now();
    get_sketch_names(sigFileList);
    read_sketches();

    auto end_read = std::chrono::high_resolution_clock::now();
    cout << "Time taken to read the sketches: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_read - start_program).count() << " milliseconds" << endl;

    // sort genome_id_size_pairs by size
    cout << "Sorting genome_id_size_pairs by size..." << endl;
    sort(genome_id_size_pairs.begin(), genome_id_size_pairs.end(), [](const pair<int, int>& a, const pair<int, int>& b) {
        return a.second < b.second;
    });

    auto end_sort = std::chrono::high_resolution_clock::now();
    cout << "Time taken to sort genome_id_size_pairs: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_sort - end_read).count() << " milliseconds" << endl;

    // show first 10 genome_id_size_pairs
    cout << "First 10 genome_id_size_pairs:" << endl;
    for (int i = 0; i < 10; i++) {
        cout << genome_id_size_pairs[i].first << " " << genome_id_size_pairs[i].second << endl;
    }

    // read the similar info
    cout << "Reading similar info..." << endl;
    read_similar_info(simFileList, num_threads);

    auto end_sim = std::chrono::high_resolution_clock::now();
    cout << "Time taken to read similar info: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_sim - end_sort).count() << " milliseconds" << endl;

    // start processing
    cout << "Start processing..." << endl;
    vector<int> selected_genome_ids;
    for (int i = 0; i < num_sketches; i++) {
        cout << "Processing " << i << "..." << endl;
        int genome_id_this = genome_id_size_pairs[i].first;
        int size_this = genome_id_size_pairs[i].second;
        bool select_this = true;
        for (int j = 0; j < similars[genome_id_this].size(); j++) {
            int genome_id_other = similars[genome_id_this][j];
            int size_other = genome_id_size_pairs[genome_id_other].second;
            if (size_other > size_this) {
                select_this = false;
                break;
            }
        }
        if (select_this) {
            cout << "Selected " << i << endl;
            selected_genome_ids.push_back(genome_id_this);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    cout << "Time taken for processing: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - end_sim).count() << " milliseconds" << endl;

    // write the selected genome ids to file
    ofstream outfile(outFileName);
    for (int i = 0; i < selected_genome_ids.size(); i++) {
        int genome_id = selected_genome_ids[i];
        string sketch_name = sketch_names[genome_id];
        outfile << sketch_name << endl;
    }
    outfile.close();


    return 0;

}