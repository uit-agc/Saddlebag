// Copyright 2019 Saddlebag Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef UTILS_HPP
#define UTILS_HPP

// These flags are to control various features of Saddlebag
#define INITIAL_RESERVE_SIZE 500
// Set to [1-10] for how frequently call upcxx::progress()
#define UPCXX_PROGRESS_INTERVAL 5
// Set to 0 to turn off all messages, and [1-6] for detailed messages
#define SADDLEBAG_DEBUG 3
// Set to true to use upcxc::local() optimization
// Set to false for testing rget on the same node
#define UPCXX_GPTR_LOCAL_ON true
// Number of iterations before measurement starts for benchmarking
#define BENCH_WARMUP_ITER 3

// Use robin hood hashing for storing items (instead of std::unordered_map)
#define ROBIN_HASH true
// Use CityHash for distributing items to partitions (instead of simple modulo operator)
#define CITY_HASH 42002
// Use xxHash for distributing items to partitions
#define XX_HASH 42001
// Use simple modulo operator
#define MODULO_HASH 42003
// Which hash function to use to find distribution of items to partitions
#define DISTRIB_HASH MODULO_HASH

// Following flags are only for debugging
//
// Set to true for testing only communication overhead
#define DEBUG_COMM_BENCHMARK false
// Set to true to measure additional times
#define DEBUG_TIME_MEASUREMENTS true
// Set to true to not create new items on receivig push
#define DEBUG_DISABLE_CREATE_ON_PUSH true
// Set to true to ignore pushing empty messages
#define DEBUG_IGNORE_PUSH_EMPTY_MSG false

#if DISTRIB_HASH == XX_HASH
extern "C" {
#include "xxhash.h"
};
#elif DISTRIB_HASH == CITY_HASH
#include "city.h"
#endif

//Define hash functions in the std namespace so that they can be used in unordered_map
namespace std {

template <>
struct hash<vector<string>> {
std::size_t operator()(const vector<string>& k) const {
    string tmp = "";

    for (auto s : k)
    {
        tmp += s;
    }

    return hash<string>{}(tmp);
}
};

template <>
struct hash<pair<int,int>> {
std::size_t operator()(const pair<int,int>& k) const {
    int tmp = k.first + k.second;
    return hash<int>{}(tmp);
}
};

}

namespace saddlebags {

// Wrappers around UPC++ functions

inline void barrier() { upcxx::barrier(); }

inline void barrier(bool is_global) {
    if (is_global) {
        upcxx::barrier();
    } else { // LOCAL_PUSH_OPT
        upcxx::barrier(upcxx::local_team());
    }
}

inline void finalize() { upcxx::finalize(); }

inline void init() { upcxx::init(); }

inline int rank_me() { return (int) upcxx::rank_me(); }

inline int rank_n() { return (int) upcxx::rank_n(); }


/**
 *
 */
inline void progress(int i = 0, int interval = UPCXX_PROGRESS_INTERVAL) {
    // periodically call progress to allow incoming RPCs to be processed
    if (i % interval == 0) {
        upcxx::progress();
    }
}

#if DISTRIB_HASH == XX_HASH
inline unsigned long long xxhash_64(const void* buffer, const std::size_t & length) {
    unsigned long long const seed = 2654435761;   /* From Knuth's Multiplicative Hash */
    unsigned long long const hash = XXH64(buffer, length, seed);
    return hash;
}

inline unsigned int xxhash_32(const void* buffer, const std::size_t & length) {
    unsigned int const seed = 2654435761;   /* From Knuth's Multiplicative Hash */
    unsigned int const hash = XXH32(buffer, length, seed);
    return hash;
}
#endif

/**
 *
 * @param key
 * @return
 */
inline std::size_t distrib_hash(const std::string & key) {
#if DISTRIB_HASH == XX_HASH
    return (std::size_t) xxhash_64(key.c_str(), key.length());
#elif DISTRIB_HASH == CITY_HASH
    return (std::size_t) CityHash32(key.c_str(), key.length());
#elif DISTRIB_HASH == MODULO_HASH
    // TODO: Convert string to integer for modulo operation
#else

#endif
}

/**
 *
 * @param key
 * @return
 */
inline std::size_t distrib_hash(const unsigned int & key) {
#if DISTRIB_HASH == XX_HASH || DISTRIB_HASH == CITY_HASH
    return distrib_hash(std::to_string(key));
    // TODO: How to pass int directly to XXH64?
#elif DISTRIB_HASH == MODULO_HASH
    return key;
#else
    return key;
#endif
}

/**
 * SendingModes define the behaviour of outgoing messages from Items
 */
enum SendingMode {
    Combining
};

}

#endif
