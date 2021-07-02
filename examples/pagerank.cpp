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

/*
 * PageRank Benchmark
 */

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <sched.h>
#include <sstream>
#include <string>
#include <unistd.h>

#include "saddlebags.hpp"

#define VERTEX_TABLE 0
#define DEBUG true // Show descriptive messages
#define LOG true // Show elapsed time in csv format
#define DEBUG_DETAILED false
#define PROGRESS false // show progress bar when reading file
#define SEP ','
#define EXPORT_FORMATS false
#define EXPORT_FORMATS_NUM_NEIGHBORS true
#define EXPORT_FORMATS_SEP '\t'
#define INITIAL_RESERVE_SIZE_MAX_EDGES 50

template<class Tk, class Ok, class Mt>
class Vertex : public saddlebags::Item<Tk, Ok, Mt> {
  public:

    int vertex_id = 0;
    float page_rank = 1;
    float new_page_rank = 0;
    std::vector<int> links;

    void add_link(int new_link) {
        this->links.emplace_back(new_link);
    }

    void do_work() override {
        if (links.size() > 0) {
            for(auto it : this->links) {
                float pr = page_rank / ((float) links.size());
                if (pr > 0 || true) {
                    this->push(VERTEX_TABLE, it, pr);
                }

                if (pr <= 0 && SADDLEBAG_DEBUG > 5) {
                    std::cout << "[Rank " << saddlebags::rank_me() << "] "
                              << "[Vertex " << vertex_id << "] "
                              << "Page rank value is zero."
                              << std::endl;
                }
            }
        }
    }

    void on_push_recv(Mt val) override {
        new_page_rank +=  0.15 * page_rank + 0.85 * val;
    }

    void before_work() override {
        if (new_page_rank > 0) {
            page_rank = new_page_rank;
        }

        new_page_rank = 0;
    }

    void finishing_work() override {
    }

    void on_create() override {
        page_rank = 1;
        new_page_rank = 0;
    }

    void Item() {
        links.reserve(INITIAL_RESERVE_SIZE_MAX_EDGES);
    }

};

template class Vertex<uint8_t, unsigned int, float>;
template class saddlebags::Worker<uint8_t, unsigned int, float>;
typedef class saddlebags::Worker<uint8_t, unsigned int, float> WorkerPageRank;

namespace upcxx {
template<class Tk, class Ok, class Mt>
struct is_definitely_trivially_serializable<Vertex<Tk, Ok, Mt>> : std::true_type {};
}

/*********************************************************************************************
  Below are miscellaneous functions
**********************************************************************************************/

/*
 * Get File Name from a Path with or without extension
 */
std::string getFileName(std::string filePath, bool withExtension = true, char seperator = '/')
{
    // Get last dot position
    std::size_t dotPos = filePath.rfind('.');
    std::size_t sepPos = filePath.rfind(seperator);

    if(sepPos != std::string::npos) {
        return filePath.substr(sepPos + 1, filePath.size() - (withExtension || dotPos != std::string::npos ? 1 : dotPos) );
    }

    return "";
}

/*
 * Print maximum PageRank information
 */
std::string get_max_pagerank(WorkerPageRank *worker, int iter = 0, bool detailed = true) {

    std::ostringstream s;
    float max_pr = 0;
    int max_pr_id = 0;

    return "";
    for (auto vertex : worker->iterate_table<Vertex>(VERTEX_TABLE)) {
        int vertex_id = vertex.first;
        auto vertex_pr = vertex.second->page_rank;
        auto links = vertex.second->links;

        if (vertex_pr >= max_pr) {
            max_pr = vertex_pr;
            max_pr_id = vertex_id;
        }
    }

    if (detailed) {
        s << std::endl
        << "[Iter " << iter << "]"
        << " Max ID: " << max_pr_id
        << ", Max PageRank:" << max_pr;
    } else {
        s << max_pr << SEP << max_pr_id;
    }

    return s.str();
}

// Return total number of vectors
int get_vertex_count(WorkerPageRank *worker){

    int total_vertex = 0;

  return 0;
    for (auto vertex : worker->iterate_table<Vertex>(VERTEX_TABLE)) {
        int vertex_id = vertex.first;
        auto links = vertex.second->links;

        if (links.size() > 0) {
            total_vertex++;
        }
    }

    return total_vertex;
}

/*
 * Export graph data in different formats
 */
std::string export_vectors(WorkerPageRank *worker, std::string out_file_path = "graph.txt") {

    std::ostringstream output;
    int total_vertex = 0;
    int total_edges = 0;

    std::ofstream out_file(out_file_path, std::ofstream::out);

    if (! out_file) {
        return "";
    }

    return "";
    for (auto vertex : worker->iterate_table<Vertex>(VERTEX_TABLE)) {

        output.str(std::string()); // clear buffer

        int vertex_id = vertex.first;
        output << vertex_id;

        auto vertex_pr = vertex.second->page_rank;
        auto links = vertex.second->links;

        if (links.size() == 0) {
            continue;
        }

        if (EXPORT_FORMATS_NUM_NEIGHBORS) {
            output << EXPORT_FORMATS_SEP << links.size();
        }

        for (auto link: links) {
            output << EXPORT_FORMATS_SEP << link;
            total_edges++;
        }

        total_vertex ++;
        out_file << output.str() << std::endl;
        out_file.flush();
    }

    if (DEBUG_DETAILED) {
        std::cout << "Vertex: " << output.str() << std::endl;
    }

    out_file.close();
    return out_file_path;
}

/*
 *
 */
int load_data(WorkerPageRank *worker, std::string data_file,
              int& total_vertices, int& total_edges, int& total_rows, int& rows_skipped) {

    int rank_me_ = saddlebags::rank_me();
    bool isRankRoot = rank_me_ == 0;
    int rank_n_ = upcxx::rank_n();
    std::string line;
    std::string vertex_str;
    int prog_counter = 0;
    int format_neighbor_start_index = -1;
    int neighbor;

    auto start_time = std::chrono::high_resolution_clock::now();

    if(isRankRoot && DEBUG) {
        std::cout << "Loading data from: " << data_file << std::endl;
    }

    std::ifstream infile(data_file);
    if (!infile.good()) {
        if(isRankRoot) {
            std::cerr << "ERROR: Unable to open file: " << data_file << std::endl;
        }
        return -1;
    }

    while (std::getline(infile, line)) {

        if (line.empty()) {
            rows_skipped++;
            continue;
        }

        std::stringstream ss(line);
        std::vector <std::string> tokens;

        while (ss >> vertex_str) {
            tokens.push_back(vertex_str);
        }

        if (tokens.size() < 2) {
            std::cerr << "[Rank " << rank_me_ << "] ERROR: Unable to parse vertex for: " << line << std::endl;
            rows_skipped++;
            continue;
        }

        // Possible graph file formats like: source num_neighbors neighbor_1 neighbor_2 ... neighbor_n
        if (format_neighbor_start_index <= 0) {
            if (tokens.size() >= 3) {
                format_neighbor_start_index = 2;
            } else {
                format_neighbor_start_index = 1;
            }
        }

        int vertex = stoi(tokens[0]);
        total_rows++;

        bool is_my_obj = (worker->get_partition(VERTEX_TABLE, vertex) == rank_me_);

        if (is_my_obj) {
            auto new_obj = worker->add_item<Vertex>(VERTEX_TABLE, vertex);

            if (new_obj != nullptr) {
                total_vertices++;

                for(int i = format_neighbor_start_index; i < tokens.size(); i++) {
                    neighbor = stoi(tokens[i]);
                    new_obj->add_link(neighbor);
                    new_obj->vertex_id = vertex;
                    total_edges++;

                    // Add edge vertex too, if it's on same partition
                    if (worker->get_partition(VERTEX_TABLE, neighbor) == rank_me_) {
                        auto link_obj = worker->add_item<Vertex>(VERTEX_TABLE, neighbor);
                        link_obj->vertex_id = neighbor;
                    }
                }

                if (total_vertices == 1 && DEBUG_DETAILED) {
                    std::cout << "[Rank " << upcxx::rank_me() << "] Inserted first vertex <" << vertex << ">."
                              << std::endl;
                }
            }
        }

        if (PROGRESS && isRankRoot && !DEBUG_DETAILED && total_rows % 50000 == 0) {
            std::cout << ".";

            if (prog_counter == 0) {
                std::cout << "." << std::flush;
            }

            if (prog_counter++ > 60) {
                std::cout << "." << std::endl;
                prog_counter = 0;
            }
        }

        if (DEBUG_DETAILED && total_rows % 500000 == 0 && isRankRoot) {
            auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            std::cout << "[Rank " << upcxx::rank_me() << "] "
                      << ctime(&timenow)
                      << "Processing objects: " << total_vertices << " / " << total_rows << std::endl;
        }
    }

    if (PROGRESS && isRankRoot && !DEBUG_DETAILED) {
        std::cout << "." << std::endl;
    }

    infile.close();

    if(DEBUG_DETAILED) {
        auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::cout << "[Rank " << upcxx::rank_me() << "] "
                  << ctime(&timenow)
                  << "Inserted objects: " << total_vertices
                  << " (Out of total objects: " << total_rows << ")" << std::endl;
    }

    upcxx::barrier();
    auto end_data_gen = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time_data_gen = end_data_gen - start_time;
    float duration_data_gen = elapsed_time_data_gen.count() * 1e+3;

    if (DEBUG && isRankRoot) {
        auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::cout << "[Rank " << upcxx::rank_me() << "] "
                  << ctime(&timenow)
                  << "Input file loaded with " << total_rows
                  << " objects in " << duration_data_gen << " ms." << std::endl;
    }
}

/*
 *
 */
int main(int argc, char* argv[]) {

    std::string data_file = "data/pagerank/simple_graph.txt";
    int iterations = 3;
    int max_size = 10000;
    bool print_usage = false;

    if (argc > 1) {
        data_file = argv[1];
    } else {
        print_usage = true;
    }

    if (argc > 2) {
        iterations = atoi(argv[2]);
    }

    if (argc > 3) {
        max_size = atoi(argv[3]);
    }

    saddlebags::init();
    bool isRankRoot = ( saddlebags::rank_me() == 0 );
    int rank_me = saddlebags::rank_me();
    int rank_n = upcxx::rank_n();
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    int cpu = -1;
    cpu = sched_getcpu();


    if (DEBUG && upcxx::rank_me() == 0) {
        upcxx::team & local_team = upcxx::local_team();
        int total_nodes = (int) upcxx::rank_n() / local_team.rank_n();
        total_nodes += upcxx::rank_n() % local_team.rank_n() == 0 ? 0 : 1;
        std::cout << "[Rank " << upcxx::rank_me() << "] "
                  << "Usage: " << getFileName(argv[0]) << " <Path> <Iterations> <Buffer Size>" << std::endl;
        std::cout << "[Rank " << upcxx::rank_me() << "] "
                  << "Process " << upcxx::rank_me()
                  << " out of " << upcxx::rank_n() << "."
                  << " Node "<< hostname << " (out of " << total_nodes << " nodes)."
                  << " CPU " << cpu << "." << std::endl;
    }

    // Record start time
    upcxx::barrier();
    auto start_time = std::chrono::high_resolution_clock::now();
    WorkerPageRank* worker = saddlebags::create_worker<uint8_t, unsigned int, float>(max_size);
    worker->add_table<Vertex>(VERTEX_TABLE);

    int total_vertices = 0;
    int total_edges = 0;
    int total_rows = 0;
    int rows_skipped = 0;

    if (!DEBUG_COMM_BENCHMARK) {
        load_data(worker, data_file, total_vertices, total_edges, total_rows, rows_skipped);
    }

    upcxx::barrier();
    auto end_data_gen = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time_data_gen = end_data_gen - start_time;
    double duration_data_gen = elapsed_time_data_gen.count() * 1e+3;

    // First cycle to load data (and warm up the cache)
    worker->cycle(1);
    auto end_data_transfer = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time_transfer = end_data_transfer - end_data_gen;
    double duration_transfer = elapsed_time_transfer.count() * 1e+3;

    if (DEBUG && isRankRoot) {
        auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::cout << "[Rank " << upcxx::rank_me() << "] "
                  << ctime(&timenow)
                  << "Data distributed to " << rank_n
                  << " processes in " << duration_transfer << " ms." << std::endl;
    }
    worker->cycle(BENCH_WARMUP_ITER - 1);

    // Next K cycles (iterations) to calculate PageRank
    auto start_time_proc = std::chrono::high_resolution_clock::now();
    worker->cycle(iterations);

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time_total = end_time - start_time;
    double duration_total = elapsed_time_total.count() * 1e+3;

    std::chrono::duration<double> elapsed_time_proc = end_time - start_time_proc;
    double duration_proc = elapsed_time_proc.count() * 1e+3;

    // Show the success message on completion
    if (DEBUG && isRankRoot) {
        std::cout << (worker->error == 0 ? "SUCCESS" : "ERROR") << ": "
                  << "PageRank finished in time: " << duration_proc << " milliseconds"
                  << " (" << duration_proc / (60 * 1000) << " minutes)"
                  << ", Ranks: " << rank_n
                  << ", Total Objects: " << total_rows << std::endl;
        std::cout << "benchmark,platform,nodes,processes,dataset,vertices,edges,iterations,"
                  << "processing time (ms),total time (ms),data load time (ms),data transfer time (ms),"
                  << "sending mode,replication,configuration 1,configuration 2,upcxx version,"
                  << "max page rank value,max page rank vertex,rows,rows (skipped)"
                  << std::endl;
    }

    if (EXPORT_FORMATS && isRankRoot) {
        export_vectors(worker);
    }

    // Log the elapsed execution time in csv format
    if (LOG && isRankRoot) {
        std::string platform = "SaddlebagX";
        std::string config = "";

        if (SADDLEBAG_DEBUG > 1) {
            // platform += " (Debug Mode)";
            // config = "Robin Map and Modulo Hash";
        }

        if (worker->error != 0) {
            config += "Error (" + std::to_string(worker->error) + ")";
        }

        std::cout << "PageRank" << SEP
                  << platform << SEP
                  << worker->total_nodes << SEP
                  << rank_n << SEP
                  << getFileName(data_file) << SEP
                  << total_vertices << SEP
                  << total_edges << SEP
                  << iterations << SEP
                  << duration_proc << SEP
                  << duration_total << SEP
                  << duration_data_gen << SEP
                  << duration_transfer << SEP
                  << "" << SEP
                  << "0" << SEP
                  << config << SEP
                  << "-O3 flag" << SEP
                  << "upcxx-2018.9.0" << SEP
                  << get_max_pagerank(worker, 0, false) << SEP
                  << total_rows << SEP
                  << rows_skipped
                  << std::endl;
    }

    // Close down UPC++ runtime
    saddlebags::destroy_worker(worker);
    saddlebags::finalize();
    return 0;
}
