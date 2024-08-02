#include <iostream>
#include <ctime>
#include <cstring>
#include <fstream>

using namespace std;

#define ROTL64(x, y) rotl64(x, y)
#define BIG_CONSTANT(x) (x)

inline uint64_t getblock64(const uint64_t *p, int i)
{
    return p[i];
}

inline uint64_t rotl64(uint64_t x, int8_t r)
{
    return (x << r) | (x >> (64 - r));
}

inline uint64_t fmix64(uint64_t k)
{
    k ^= k >> 33;
    k *= BIG_CONSTANT(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;

    return k;
}

void MurmurHash3_x64_128(const void *key, const int len,
                         const uint32_t seed, void *out)
{
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = len / 16;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    const uint64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
    const uint64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);

    //----------
    // body

    const uint64_t *blocks = (const uint64_t *)(data);

    for (int i = 0; i < nblocks; i++)
    {
        uint64_t k1 = getblock64(blocks, i * 2 + 0);
        uint64_t k2 = getblock64(blocks, i * 2 + 1);

        k1 *= c1;
        k1 = ROTL64(k1, 31);
        k1 *= c2;
        h1 ^= k1;

        h1 = ROTL64(h1, 27);
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729;

        k2 *= c2;
        k2 = ROTL64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

        h2 = ROTL64(h2, 31);
        h2 += h1;
        h2 = h2 * 5 + 0x38495ab5;
    }

    //----------
    // tail

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
        k2 = ROTL64(k2, 33);
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
        k1 = ROTL64(k1, 31);
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

    const char* s = sequence.c_str();

    int kmer_length = 21;
    int num_kmers = strlen(s) - kmer_length + 1;
    // create an array of of uint64_t, dimensions = num_kmers x 2
    uint64_t *out = new uint64_t[2 * num_kmers];

    uint32_t seed = 0;

    double start_time = clock();
    for (int i = 0; i < num_kmers; i++)
    {
        MurmurHash3_x64_128(s + i, kmer_length, seed, out + 2 * i);
    }
    double end_time = clock();

    std::cout << "Time taken: " << (end_time - start_time) / CLOCKS_PER_SEC << std::endl;

    // write the output to a file
    std::ofstream outfile(out_filename);
    for (int i = 0; i < num_kmers; i++)
    {
        string kmer = sequence.substr(i, kmer_length);
        outfile << kmer << " " << out[2 * i] << " " << out[2 * i + 1] << endl;
    }

    return 0;
}
