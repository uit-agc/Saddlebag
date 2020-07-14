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

/**
 * Sparse matrix-vector multiplication example
 *
 * Using Saddlebag API
 *
 * Given:
 *      P number of partitions,
 *      M x M size of matrix, and
 *      NNZPR number of non-zero elements per row
 * Create 1 worker, and 1 table, and 1 Item type
 * Create P number of item objects
 *      where each item is of type <Matrix>,
 *      and has ID value (equivalent to upcxx::rank_me())
 *      and Item 0 already has vector x on initialization
 * Item 0 pushes vector x to all the Items
 *      Run a cycle to distribute Item objects
 * Run a cycle with instantiating do_work()
 *      Each [0,...,n-1] item calculates SpMV,
 *      using locally generated rows in M/P x M matrix
 * Omitted: Results aren't copied back to either item 0 or all items
 */

#include "saddlebags.hpp"

#include <iostream>
#include <fstream>
#include <string>
#include <algorithm> // for generate()
#include <cstdlib> // for rand()
#include <chrono>  // for high_resolution_clock
#include <unistd.h>
#include <sched.h>
#include <limits>

#define MATRIX_TABLE 0
#define ITEM_ROOT 0
#define DEBUG true
#define LOG true
#define DEBUG_DETAILED false
#define SEP ','

std::size_t NNZPR = 64;  // 512;  // number of non-zero elements per row
std::size_t M = 2048;  // 1024*1024; // matrix size (M-by-M)

// Utility Functions (from UPC++ example)
template<typename T>
static void print_sparse_matrix(T *A,
                                size_t *col_indices,
                                size_t *row_offsets,
                                size_t nrows)
{
    size_t i, j, k;

    for (i = 0; i < nrows; i++) {
        for (j = 0; j < col_indices[i]; j++) {
            printf("[%lu] %lg\t ", j, A[j]);
        }
        printf("\n");
    }
}

int random_int(const int M=1024)
{
    //void srand ( unsigned int seed );
    return rand() % M;
}

double random_double()
{
    long long tmp;

    tmp = rand();
    tmp = (tmp << 32) | rand();
    return (double) tmp;
}

// if the compiler doesn't support the "restrict" type qualifier then
// pass "-Drestrict " to CXXFLAGS.
#define restrict /* Don't have the restrict keyword */
template<typename T>
void spmv(const T * restrict A,
          const size_t * restrict col_indices,
          const size_t * restrict row_offsets,
          size_t nrows,
          const T * restrict x,
          T * restrict y)

{
    // Record start time
    auto start_time = std::chrono::high_resolution_clock::now();
    int ops = 0;
    long double sum_overall = 0;

    // len(row_offsets) == nrows + 1
#pragma omp parallel for firstprivate(A, col_indices, row_offsets, x, y)
    for (size_t i = 0; i < nrows; i++) {
        T tmp = 0;
        for (size_t j = row_offsets[i]; j < row_offsets[i + 1]; j++) {
            tmp += A[j] * x[col_indices[j]];
            ops++;
        }
        y[i] = tmp;
        sum_overall += tmp;
    }

    if (upcxx::rank_me() == 0 && DEBUG_DETAILED) {
        // Record end time
        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed_time = end_time - start_time;
        double duration = elapsed_time.count() * 1e+3;
        std::cout << "[Rank " << saddlebags::rank_me() << "]"
            << " Calculated SpMV in " << duration << " milliseconds." 
            << " Rows: " << nrows << "."
            << " Operations: " << ops << "."
            << " Overall local sum: " << sum_overall << std::endl;
    }
}

template<typename T>
void gen_matrix(T *my_A,
                size_t *my_col_indices,
                size_t *my_row_offsets,
                size_t nrows)
{
    int num_nnz = NNZPR * nrows;
    long double sum_overall = 0;

    // generate the sparse matrix
    // each place only generates its own portion of the sparse matrix
    my_row_offsets[0] = 0;
    for (size_t row = 0; row < nrows; row++) {
        my_row_offsets[row + 1] = (row + 1) * NNZPR;
    }

    // Generate the data and the column indices
    for (int i = 0; i < num_nnz; i++) {
        my_col_indices[i] = random_int();
        my_A[i] = random_double();
        sum_overall += my_A[i];
    }

    if (upcxx::rank_me() == 0 && DEBUG_DETAILED) {
        std::cout << "[Rank " << upcxx::rank_me() << "]"
                  << " Rows: " << nrows << "."
                  << " Num_nnz: " << num_nnz << "."
                  << " Overall local sum: " << sum_overall << std::endl;
    }
}

void print_usage(char *s)
{
    assert(s != NULL);
    // printf("usage: %s (matrix_size) (nonzeros_per_row) \n", s);
    std::cerr << "Usage: " << s << " <Matrix Size> <Non-Zeros per Row>" << std::endl;
}

template<class Tk, class Ok, class Mt>
class Matrix : public saddlebags::Item<Tk, Ok, Mt> {

public:

    std::vector<double> x_vector;

    double *A ;
    size_t *col_indices;
    size_t *row_offsets;
    double *x;
    double *y;
    int nrows;
    int rank = 0;
    int P;

    void on_create() override {
    }

    //For each cycle
    void do_work() override
    {
        double* my_x = this->x_vector.data();

        // perform matrix multiplication in parallel
        spmv(this->A, this->col_indices, this->row_offsets, this->nrows, my_x, this->y);

        // Verify that all ranks have identical values
        if(DEBUG_DETAILED) {
            long double sum = 0;
            double *my_x = this->x_vector.data();
            for (int i = 0; i < this->x_vector.size(); i++) {
                sum += my_x[i];
            }
            std::cout << "[Rank " << saddlebags::rank_me() << "/" << this->rank << "] Sum of vector: " << sum << std::endl;
        }
    }

    void foreign_push(Mt val) override {
        // Store the vector x received from Item 0
        this->x_vector = val;
    }

    Mt foreign_pull(int tag) override {
        // Do nothing
        return this->x_vector;
    }

    void refresh() override {
    }

    void returning_pull(saddlebags::Message<Tk, Ok, Mt> returning_message) override {
    }

    void finishing_work() override {
    }
};

namespace upcxx {
  template<class Tk, class Ok, class Mt>
  struct is_definitely_trivially_serializable<Matrix<Tk, Ok, Mt>> : std::true_type {};
}

int main(int argc, char* argv[]) {

    // parsing the command line arguments
    if(argc > 1) {

        if (strcmp(argv[1], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        }

        int tmp = std::stoi(argv[1]);
        if (tmp > 2) {
            M = tmp;
        }

    } else {
        print_usage(argv[0]);
    }

    if (argc > 2) {
        int tmp = std::stoi(argv[2]);
        if (tmp > 0) {
            NNZPR = tmp;
        }
    }

    saddlebags::init();
    int ranks_per_node = 16;
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    int cpu = -1;
    cpu = sched_getcpu();

    if (DEBUG_DETAILED) {
        std::cout << "[Rank " << upcxx::rank_me() << "] "
            << "Process " << upcxx::rank_me()
            << " out of " << upcxx::rank_n() << "." 
            << " Node "<< hostname << "." 
            << " CPU " << cpu << "." << std::endl;
    }

    if (upcxx::rank_me() == 0 && DEBUG_DETAILED) {
        std::cout << "[Rank " << upcxx::rank_me() << "]"
            << " M: " << M
            << ", NNZPR: " << NNZPR << std::endl;
    }

    upcxx::barrier();
    // Record start time
    auto start_time = std::chrono::high_resolution_clock::now();

    int items_per_rank = 1;
    int P = upcxx::rank_n() * items_per_rank;
    int nrows = M / P;  // assume M can be divided by P

    /* Create a worker and add one table for the vector x
     * Table is indexed with default value of 0
     * Matrix blocks are indexed with the rank of the node */
    auto worker = saddlebags::create_worker<int, int, std::vector<double>>(Combining);
    // worker->ordered_pulls = false; // Message ordering not required
    // worker->set_replication(3); // Replication can be omitted
    saddlebags::add_table<saddlebags::CyclicDistributor, Matrix>(worker, MATRIX_TABLE, true);
    // Which modes in communication are needed
    worker->set_modes( true, false, false, false, false, true );
    // allocate memory
    double *x = new double[M];

    // Seed the random generator
    if(DEBUG_DETAILED && false) {
        srand( 0 );
    } else {
        srand( time(NULL) );
    }

    worker->set_broadcast(MATRIX_TABLE, ITEM_ROOT, true);
    std::vector<double> v_broadcast;

    // Create the Items
    for (int i = 0; i < P; i++) {
        // Create each item on its respective partition
        if(worker->get_partition(MATRIX_TABLE, i) == saddlebags::rank_me()) {

            auto my_matrix = saddlebags::insert_and_return<Matrix>(worker, MATRIX_TABLE, i);
            my_matrix->rank = i;
            my_matrix->nrows = nrows;
            my_matrix->P = P;

            // allocate memory
            my_matrix->A = new double[NNZPR * nrows];
            my_matrix->col_indices = new size_t[NNZPR * nrows];
            my_matrix->row_offsets = new size_t[nrows + 1];
            my_matrix->x = new double[M];
            my_matrix->y = new double[nrows];

            // generate matrix
            gen_matrix(my_matrix->A, my_matrix->col_indices, my_matrix->row_offsets, my_matrix->nrows);

            // Item 0 creates an array for x
            // then will broadcast the vector containing x to all the other items
            if(i == ITEM_ROOT) {
                // Generate the data for x
                std::generate(x, x + M, random_double);
                std::vector<double> v(x, x + M);
                // Insert the vector x into table on first item
                my_matrix->x_vector = v;
                v_broadcast = v;

                // Push vector x to all the items
                my_matrix->broadcast( MATRIX_TABLE, ITEM_ROOT, my_matrix->x_vector );
                // for (int j = 1; j < P; j++) {
                //    my_matrix->push( MATRIX_TABLE, j, my_matrix->x_vector );
                // }

            } else {
                // Initialize all zeros for x, if needed to avoid random values
                std::fill(x, x + M, 0);
                std::vector<double> v(x, x + M);
                // Insert the vector x into table on first item
                my_matrix->x_vector = v;
            }
        }
    }

    // worker->broadcast_direct( MATRIX_TABLE, ITEM_ROOT, v_broadcast );
    upcxx::barrier();
    auto end_data_gen = std::chrono::high_resolution_clock::now();

    // Perform cycle without work (communication only) to broadcast vector x
    saddlebags::cycle(worker, false);
    worker->set_broadcast(MATRIX_TABLE, ITEM_ROOT, false);
    // upcxx::barrier(); // called inside cycle()
    auto end_data_transfer = std::chrono::high_resolution_clock::now();

    // Perform cycle with work, to calculate SpMV
    saddlebags::cycle(worker, true);

    // wait until all ranks have calculated result
    // upcxx::barrier(); // called inside cycle()

    delete [] x;

    // Record end time
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time_total = end_time - start_time;
    double duration_total = elapsed_time_total.count() * 1e+3;

    std::chrono::duration<double> elapsed_time_proc = end_time - end_data_gen;
    double duration_proc = elapsed_time_proc.count() * 1e+3;

    std::chrono::duration<double> elapsed_time_data_gen = end_data_gen - start_time;
    double duration_data_gen = elapsed_time_data_gen.count() * 1e+3;

    std::chrono::duration<double> elapsed_time_transfer = end_data_transfer - end_data_gen;
    double duration_transfer = elapsed_time_transfer.count() * 1e+3;

    // Show the success message on completion
    if(saddlebags::rank_me() == 0 && DEBUG) {
        std::cout << "SUCCESS: "
                  << "SpMV finished in time: " << duration_proc << " milliseconds\n"
                  << "# of non-zero elements: " << NNZPR * M
                  << ", M: " << M
                  << ", np: " << P
                  << ", ranks: " << upcxx::rank_n() << std::endl;
        std::cout << "benchmark,platform,nodes?,ranks,items,M,NNZPR,"
                  << "processing time (ms),total time (ms),data generation time (ms),data transfer time (ms)" << std::endl;
    }

    // Log the elapsed execution time in csv format
    if(saddlebags::rank_me() == 0 && LOG) {
        std::cout << "SpMV" << SEP
                  << "Saddlebag" << SEP
                  << (int) upcxx::rank_n() / ranks_per_node << SEP
                  << upcxx::rank_n() << SEP
                  << P << SEP
                  << M << SEP
                  << NNZPR << SEP
                  << duration_proc << SEP
                  << duration_total << SEP
                  << duration_data_gen << SEP
                  << duration_transfer
                  << std::endl;
    }

    saddlebags::finalize();
    return 0;
}
