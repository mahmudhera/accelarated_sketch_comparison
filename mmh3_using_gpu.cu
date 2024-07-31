#include <cuda_runtime.h>
#include <iostream>
#include <cstdint>
#include <cstring>

__device__ uint64_t rotateLeft(uint64_t x, int r) {
    return (x << r) | (x >> (64 - r));
}

__device__ uint64_t getblock64(const uint64_t* p, int i) {
    return p[i];
}

__device__ uint64_t fmix64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccd;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53;
    k ^= k >> 33;
    return k;
}

__device__ void murmurhash3_x64_128(const void* key, const int len, const uint32_t seed, void* out) {
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 16;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    const uint64_t c1 = 0x87c37b91114253d5;
    const uint64_t c2 = 0x4cf5ad432745937f;

    const uint64_t* blocks = (const uint64_t*)(data);

    for (int i = 0; i < nblocks; i++) {
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

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 16);

    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch (len & 15) {
    case 15: k2 ^= ((uint64_t)tail[14]) << 48;
    case 14: k2 ^= ((uint64_t)tail[13]) << 40;
    case 13: k2 ^= ((uint64_t)tail[12]) << 32;
    case 12: k2 ^= ((uint64_t)tail[11]) << 24;
    case 11: k2 ^= ((uint64_t)tail[10]) << 16;
    case 10: k2 ^= ((uint64_t)tail[9]) << 8;
    case 9: k2 ^= ((uint64_t)tail[8]) << 0;
        k2 *= c2;
        k2 = rotateLeft(k2, 33);
        k2 *= c1;
        h2 ^= k2;

    case 8: k1 ^= ((uint64_t)tail[7]) << 56;
    case 7: k1 ^= ((uint64_t)tail[6]) << 48;
    case 6: k1 ^= ((uint64_t)tail[5]) << 40;
    case 5: k1 ^= ((uint64_t)tail[4]) << 32;
    case 4: k1 ^= ((uint64_t)tail[3]) << 24;
    case 3: k1 ^= ((uint64_t)tail[2]) << 16;
    case 2: k1 ^= ((uint64_t)tail[1]) << 8;
    case 1: k1 ^= ((uint64_t)tail[0]) << 0;
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

    ((uint64_t*)out)[0] = h1;
    ((uint64_t*)out)[1] = h2;
}

// Kernel function
__global__ void hashKernel(const void* key, int len, uint32_t seed, void* out) {
    murmurhash3_x64_128(key, len, seed, out);
}

// Host function to allocate memory and copy data
void hashOnGPU(const void* key, int len, uint32_t seed, void* out) {
    void* d_key;
    void* d_out;
    cudaMalloc(&d_key, len);
    cudaMalloc(&d_out, 16); // 128-bit output

    cudaMemcpy(d_key, key, len, cudaMemcpyHostToDevice);

    // Correct syntax for kernel launch
    hashKernel<<<1, 1>>>(d_key, len, seed, d_out);

    cudaMemcpy(out, d_out, 16, cudaMemcpyDeviceToHost);

    cudaFree(d_key);
    cudaFree(d_out);
}

int main() {
    const char* key = "ACGTGCAG";
    int len = strlen(key);
    uint32_t seed = 42;
    uint64_t out[2];

    hashOnGPU(key, len, seed, out);

    std::cout << "Hash: " << out[0] << endl << out[1] << std::endl;

    return 0;
}
