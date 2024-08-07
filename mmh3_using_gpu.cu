#include <cuda_runtime.h>
#include <iostream>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <unistd.h>
#include <string>
#include <fstream>

using namespace std;

__device__ uint64_t rotateLeft(uint64_t x, int r)
{
    return (x << r) | (x >> (64 - r));
}

__device__ uint64_t getblock64(const uint64_t *p, int i)
{
    uint8_t *pp = (uint8_t *)p;
    // access 8 bytes at a time, then convert to 64-bit integer
    return ((uint64_t)pp[i * 8 + 0] << 0) | ((uint64_t)pp[i * 8 + 1] << 8) | ((uint64_t)pp[i * 8 + 2] << 16) | ((uint64_t)pp[i * 8 + 3] << 24) | ((uint64_t)pp[i * 8 + 4] << 32) | ((uint64_t)pp[i * 8 + 5] << 40) | ((uint64_t)pp[i * 8 + 6] << 48) | ((uint64_t)pp[i * 8 + 7] << 56);
}

__device__ uint64_t fmix64(uint64_t k)
{
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccd;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53;
    k ^= k >> 33;
    return k;
}

__device__ void murmurhash3_x64_128(const void *key, const int len, const uint32_t seed, void *out)
{

    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = len / 16;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    const uint64_t c1 = 0x87c37b91114253d5;
    const uint64_t c2 = 0x4cf5ad432745937f;

    const uint64_t *blocks = (const uint64_t *)(data);

    for (int i = 0; i < nblocks; i++)
    {
        uint64_t k1 = getblock64(blocks, i * 2 + 0);
        uint64_t k2 = getblock64(blocks, i * 2 + 1);

        k1 *= c1;
        k1 = rotateLeft(k1, 31);
        k1 *= c2;
        h1 ^= k1;

        h1 = rotateLeft(h1, 27);
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729;

        k2 *= c2;
        k2 = rotateLeft(k2, 33);
        k2 *= c1;
        h2 ^= k2;

        h2 = rotateLeft(h2, 31);
        h2 += h1;
        h2 = h2 * 5 + 0x38495ab5;
    }

    const uint8_t *tail = (const uint8_t *)(data + nblocks * 16);

    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch (len & 15)
    {
    case 15:
        k2 ^= ((uint64_t)tail[14]) << 48;
    case 14:
        k2 ^= ((uint64_t)tail[13]) << 40;
    case 13:
        k2 ^= ((uint64_t)tail[12]) << 32;
    case 12:
        k2 ^= ((uint64_t)tail[11]) << 24;
    case 11:
        k2 ^= ((uint64_t)tail[10]) << 16;
    case 10:
        k2 ^= ((uint64_t)tail[9]) << 8;
    case 9:
        k2 ^= ((uint64_t)tail[8]) << 0;
        k2 *= c2;
        k2 = rotateLeft(k2, 33);
        k2 *= c1;
        h2 ^= k2;

    case 8:
        k1 ^= ((uint64_t)tail[7]) << 56;
    case 7:
        k1 ^= ((uint64_t)tail[6]) << 48;
    case 6:
        k1 ^= ((uint64_t)tail[5]) << 40;
    case 5:
        k1 ^= ((uint64_t)tail[4]) << 32;
    case 4:
        k1 ^= ((uint64_t)tail[3]) << 24;
    case 3:
        k1 ^= ((uint64_t)tail[2]) << 16;
    case 2:
        k1 ^= ((uint64_t)tail[1]) << 8;
    case 1:
        k1 ^= ((uint64_t)tail[0]) << 0;
        k1 *= c1;
        k1 = rotateLeft(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    };

    h1 ^= len;
    h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    ((uint64_t *)out)[0] = h1;
    ((uint64_t *)out)[1] = h2;
}

// Kernel function
__global__ void hashKernel(const void *input_string, int k, uint32_t seed, void *out, int num_kmers)
{
    // get the index of the thread, linear index of the thread in the thread block
    int i = threadIdx.x + blockIdx.x * blockDim.x;
    if (i >= num_kmers)
    {
        return;
    }
    murmurhash3_x64_128((char *)input_string + i, k, seed, (uint64_t *)out + 2 * i);
    
}

// Host function to allocate memory and copy data
// arguments: input_string, input_string_length, seed, out, k
void hashOnGPU(const void *input_string, int input_string_length, uint32_t seed, void *out, int k)
{
    int num_kmers = input_string_length - k + 1;
    // num_kmers = 2;

    // allocate memory on device
    void *d_key;
    void *d_out;

    double time_snap = clock();

    cudaMalloc(&d_key, input_string_length);
    cudaMalloc(&d_out, sizeof(uint64_t) * 2 * num_kmers); // two 64-bit integers for each k-mer

    double time_snap2 = clock();
    std::cout << "Time taken for memory allocation: " << (time_snap2 - time_snap) / CLOCKS_PER_SEC << std::endl;

    // copy data to device
    cudaMemcpy(d_key, input_string, input_string_length, cudaMemcpyHostToDevice);

    double time_snap3 = clock();
    std::cout << "Time taken for copying data to device: " << (time_snap3 - time_snap2) / CLOCKS_PER_SEC << std::endl;

    // determine the number of threads per block, number of blocks
    int threadsPerBlock = 256;
    int blocksPerGrid = (num_kmers + threadsPerBlock - 1) / threadsPerBlock;

    // measure start and end time
    double start_time = clock();

    // call kernel function
    hashKernel <<<blocksPerGrid, threadsPerBlock>>> (d_key, k, seed, d_out, num_kmers);

    // wait for the kernel to finish
    cudaError_t err = cudaDeviceSynchronize();
    if (err != cudaSuccess)
    {
        std::cerr << "Error: " << cudaGetErrorString(err) << std::endl;
    }

    double end_time = clock();

    std::cout << "Time taken for the kernel to run: " << (end_time - start_time) / CLOCKS_PER_SEC << std::endl;

    // copy data back to host
    cudaMemcpy(out, d_out, sizeof(uint64_t) * 2 * num_kmers, cudaMemcpyDeviceToHost);

    double time_snap4 = clock();
    std::cout << "Time taken for copying data back to host: " << (time_snap4 - end_time) / CLOCKS_PER_SEC << std::endl;

    // free memory
    cudaFree(d_key);
    cudaFree(d_out);

    double time_snap5 = clock();
    std::cout << "Time taken for freeing memory: " << (time_snap5 - time_snap4) / CLOCKS_PER_SEC << std::endl;
}

void readFASTA(const std::string &filename, std::string &header, std::string &sequence)
{
    std::ifstream infile(filename);
    if (!infile)
    {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

    std::string line;
    while (std::getline(infile, line))
    {
        if (line[0] == '>')
        {
            header = line; // Store the header line
        }
        else
        {
            sequence += line; // Append to the sequence string
        }
    }

    infile.close();
}

int main(int argc, char *argv[])
{
    // first command line argument is the fasta filename
    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " <in_filename> <out_filename>" << std::endl;
        return 1;
    }

    std::string filename = argv[1];
    std::string out_filename = argv[2];
    std::string header;
    std::string sequence;

    readFASTA(filename, header, sequence);

    const char *input_string = sequence.c_str();
    int input_string_length = strlen(input_string);
    uint32_t seed = 0;
    int k = 21;
    int num_kmers = input_string_length - k + 1;

    uint64_t *out = new uint64_t[2 * num_kmers];

    hashOnGPU(input_string, input_string_length, seed, out, k);
    // wait for the kernel to finish
    cudaDeviceSynchronize();

    /*
    std::ofstream outfile(out_filename);
    for (int i = 0; i < num_kmers; i++)
    {
        string kmer = sequence.substr(i, k);
        outfile << kmer << " " << out[2 * i] << " " << out[2 * i + 1] << std::endl;
    }
    */

    delete[] out;

    return 0;
}
