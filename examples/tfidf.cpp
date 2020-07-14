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
Term Frequency-Inverse Document Frequency algorithm

Documents are represented by their file-name in a "doc" table
Words are represented in a "term" table
A word's occurrence in a document is represented in a "termdoc" table

For each term in doc
	push 1 to termdoc(term, doc)
when termdoc is created:
	push 1 to TERM
push TERM COUNT to doc(doc)
for each TermDoc:
	pull doc to obtain Term Frequency

---------------

Now we have:
TermDoc: number of times TERM occur in DOC
Term: number of unique documents with TERM
Doc: total number of terms in Doc

1 TermDoc pulls Doc, now has number of occurrences of term in Doc, and total number of terms in Doc
2 TermDoc can now calculate term frequency
3 Term knows the number of docs with a term and can calculate idf
4 Each TermDoc pulls idf from term, and can calculate tf-idf

This approach assumes that the total number of documents is known, and fixed
*/

#include <iostream>
#include <fstream>
#include <string>
#include <cmath>
#include <ctime>
#include <chrono>  // for high_resolution_clock
#include <unistd.h>
#include <sched.h>

#include "saddlebags.hpp"

#define TERM_TABLE 0
#define DOC_TABLE 1
#define TERMDOC_TABLE 2

#define DEBUG true // Show descriptive messages
#define LOG true // Show elapsed time in csv format
#define DEBUG_DETAILED true // Show descriptive messages
#define DEBUG_SUPER_DETAILED false
#define FLUSH_AFTER_EVERY_FILE false // cycle() after reading each file
#define PROGRESS false // show progress bar when reading file
#define SEP ','

#if SADDLEBAG_VERSION < 200
#error This examples requires Saddlebag 2.0.0 or newer.
#endif

template<class Tk, class Ok, class Mt>
class TermDocObject : public saddlebags::Item<Tk, Ok, Mt> {
    public:
    float term_frequency = 0;
    float inv_doc_frequency = 0;
    float occurences = 0;

    void on_create() override {

        this->push(TERM_TABLE, {this->myItemKey[0]}, 1);
    }

    void refresh() override {
        this->occurences += 1;
    }

    void do_work() override {
        this->pull(TERM_TABLE, {this->myItemKey[0]});
        this->pull(DOC_TABLE, {this->myItemKey[1]});
    }

    void returning_pull(saddlebags::Message<Tk, Ok, Mt> returning_message) override {
        if(returning_message.src_table == DOC_TABLE) {
            term_frequency = this->occurences / (float)returning_message.value;
        }
        else if(returning_message.src_table == TERM_TABLE)
        {
            inv_doc_frequency = returning_message.value;
            this->value = term_frequency * inv_doc_frequency;
        }
    }

};

template<class Tk, class Ok, class Mt>
class TermObject : public saddlebags::Item<Tk, Ok, Mt> {
    public:
    Mt foreign_pull(int tag) override
    {
        return log((1036.0+1) / (this->value));
    }

    void foreign_push(float val) override {
        this->value += val;
    }
};

template<class Tk, class Ok, class Mt>
class DocObject : public saddlebags::Item<Tk, Ok, Mt> {
    public:

    void refresh() override {
        this->value += 1;
    }

    void foreign_push(Mt val) override {
        this->value += val;
    }

    Mt foreign_pull(int tag) override
    {
        return this->value;
    }

};

namespace upcxx {
  template<class Tk, class Ok, class Mt>
  struct is_definitely_trivially_serializable<TermDocObject<Tk, Ok, Mt>> : std::true_type {};

  template<class Tk, class Ok, class Mt>
  struct is_definitely_trivially_serializable<TermObject<Tk, Ok, Mt>> : std::true_type {};

  template<class Tk, class Ok, class Mt>
  struct is_definitely_trivially_serializable<DocObject<Tk, Ok, Mt>> : std::true_type {};
}

/*
 * Get File Name from a Path with or without extension
 * Source: https://thispointer.com/c-how-to-get-filename-from-a-path-with-or-without-extension-boost-c17-filesytem-library/
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

int main(int argc, char* argv[]) {

    std::string fileNames = "data/tfidf/filenames.txt";
    std::string dataFolder = "data/tfidf/wikidump/";

    if(argc > 2) {
        fileNames = argv[1];
        dataFolder = argv[2];
    } else {
        std::cerr << "Usage: " << argv[0] << " <File Names> <Data Folder>" << std::endl;
    }

    if (dataFolder.back() != '/' ) {
        dataFolder += "/";
    }

    upcxx::init();
    bool isRankRoot = ( saddlebags::rank_me() == 0 );
    int ranks_per_node = 16;
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    int cpu = -1;
    cpu = sched_getcpu();
    int data_transfer_cycles = 0;

    if (DEBUG_SUPER_DETAILED) {
        std::cout << "[Rank " << upcxx::rank_me() << "] "
            << "Process " << upcxx::rank_me()
            << " out of " << upcxx::rank_n() << "." 
            << " Node "<< hostname << "." 
            << " CPU " << cpu << "." << std::endl;
    }

    // Record start time
    upcxx::barrier();
    auto start_time = std::chrono::high_resolution_clock::now();

    /* 1 Create worker and tables */

    auto worker = saddlebags::create_worker<int, std::vector<std::string>, float>(Combining);

    saddlebags::add_table<TermDocObject>(worker, TERMDOC_TABLE, false);
    saddlebags::add_table<TermObject>(worker, TERM_TABLE, true);
    saddlebags::add_table<DocObject>(worker, DOC_TABLE, false);

    /* 2 Designate filenames to partitions */
    if(DEBUG && isRankRoot) {
        std::cout << "Loading file names from: " << fileNames << std::endl;
        std::cout << "Loading data from: " << dataFolder << std::endl;
    }
    std::ifstream file_list( fileNames );

    if( ! file_list.good() ) {
        if ( upcxx::rank_me() == 0 ) {
            std::cerr << "ERROR: Unable to open file: " << fileNames << std::endl;
        }
        return -1;
    }

    // The first word in the `filenames.txt` is the total number of files
    std::string file_count_str = "";
    int file_count = 0;
    int base_files_per_rank = 0;
    int my_files_per_rank = 0;
    int inserted_objects = 0;
    int files_read = 0;
    int files_processed = 0;
    int files_eof_reached = false;
    int prog_counter = 0;

    if ( file_list >> file_count_str ) {
        file_count = stoi(file_count_str);
    }

    if ( file_count == 0 ) {
        if ( upcxx::rank_me() == 0 ) {
            std::cerr << "ERROR: No files to process." << std::endl;
        }
        return -1;
    }

    // Files for each rank
    base_files_per_rank = (int) file_count / upcxx::rank_n();
    my_files_per_rank = base_files_per_rank;

    if( file_count == upcxx::rank_n() ) {

    } else if ( file_count > upcxx::rank_n() ) {

        // make sure not to straggle last rank
        if( file_count - base_files_per_rank * upcxx::rank_n() > 3 ) {
            base_files_per_rank++;
            my_files_per_rank = base_files_per_rank;

            if( upcxx::rank_me() > (int) file_count / base_files_per_rank ) {
                my_files_per_rank = 0;
            }

        } else if( upcxx::rank_me() == upcxx::rank_n() - 1  ) {
            my_files_per_rank += file_count - (base_files_per_rank * upcxx::rank_n());
        }

    } else {
        base_files_per_rank = 1;
        my_files_per_rank = ( upcxx::rank_me() < file_count ) ? 1 : 0;
    }

    if(DEBUG_DETAILED) {
        std::cout << "[Rank " << upcxx::rank_me() << "] Files to process: " << my_files_per_rank
            << " (Out of total files: " << file_count << ")" << std::endl;
    }

    // Just parse through `filenames.txt` to ignore all files *not* to be processed
    for(int i = 0; i < base_files_per_rank * upcxx::rank_me() && my_files_per_rank; i++)
    {
        std::string filename = "";
        if ( file_list >> filename ) {
            files_read++;
        } else {
            files_eof_reached = true;
            break;
        }
    }

    /* 3 Insert objects*/
    for(int i = 0; i < my_files_per_rank; i++) {
        std::string filename = "";

        if ( files_eof_reached ) {
            break;

        } else if( file_list >> filename ) {
            files_read++;

            std::string document_name = dataFolder + filename;

            if (DEBUG_SUPER_DETAILED) {
                std::cout << "[Rank " << upcxx::rank_me() << "] Loading file: " << document_name << std::endl;
            }

            std::ifstream infile(document_name);
            if (!infile.good()) {
                if (DEBUG) {
                    std::cerr << "[Rank " << upcxx::rank_me() << "] " << "ERROR: Unable to open file: " << document_name
                              << std::endl;
                }
            } else {

                files_processed++;

                while ( infile ) {
                    std::string word = "";

                    if ( infile >> word ) {
                        inserted_objects += 1;

                        //For every word in a file:
                        //1 insert an item with the document name (to count total number of words in document)
                        //2 insert an item with term/document combination (to count unique occurrences of a term)

                        insert_object(worker, DOC_TABLE, {filename});
                        insert_object(worker, TERMDOC_TABLE, {word, filename});
                    } else {
                        break;
                    }
                }
            }
        } else {
            files_eof_reached = true;
            break;
        }

        if( FLUSH_AFTER_EVERY_FILE && data_transfer_cycles < base_files_per_rank) {
            // flush all objects to other Items after every file
            saddlebags::cycle(worker, false);
            data_transfer_cycles++;
        }

        if ( PROGRESS && isRankRoot && !DEBUG_DETAILED ) {
            std::cout << ".";

            prog_counter++;

            if( prog_counter > 80 ) {
                std::cout << "." << std::endl;
                prog_counter = 0;
            }
        }
    }

    // Extra call to make sure all ranks call cycle exact same number of times
    while ( FLUSH_AFTER_EVERY_FILE && data_transfer_cycles < base_files_per_rank) {
        // flush all objects to other Items after every file
        saddlebags::cycle(worker, false);
        data_transfer_cycles++;
    }

    if ( PROGRESS && isRankRoot && !DEBUG_DETAILED ) {
        std::cout << "." << std::endl;
    }

    if(DEBUG_DETAILED) {
        auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::cout << "[Rank " << upcxx::rank_me() << "] "
                  << ctime(&timenow)
                  << "Files/Rank: " << my_files_per_rank << ", "
                  << "Files Processed: " << files_processed << ", "
                  << "Inserted Objects: " << inserted_objects << std::endl;
    }

    upcxx::barrier();
    auto end_data_gen = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time_data_gen = end_data_gen - start_time;
    double duration_data_gen = elapsed_time_data_gen.count() * 1e+3;

    if ( DEBUG && isRankRoot ) {
        auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::cout << "[Rank " << upcxx::rank_me() << "] "
                  << ctime(&timenow)
                  << "Input files loaded with " << inserted_objects
                  << " objects in " << duration_data_gen << " ms." << std::endl;
    }

    //Perform cycle without work (communication only) to create all items (and warm up the cache)
    saddlebags::cycle(worker, false);
    // upcxx::barrier(); // called inside cycle()
    auto end_data_transfer = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time_transfer = end_data_transfer - end_data_gen;
    double duration_transfer = elapsed_time_transfer.count() * 1e+3;

    if ( DEBUG && isRankRoot ) {
        auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::cout << "[Rank " << upcxx::rank_me() << "] "
                  << ctime(&timenow)
                  << "Data distributed to " << upcxx::rank_n()
                  << " processes in " << duration_transfer << " ms"
                  << " (in " << ++data_transfer_cycles << " cycles)." << std::endl;
    }

    //Perform cycle with work, to calculate tf-idf
    auto start_time_proc = std::chrono::high_resolution_clock::now();
    saddlebags::cycle(worker, true);

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time_total = end_time - start_time;
    double duration_total = elapsed_time_total.count() * 1e+3;

    std::chrono::duration<double> elapsed_time_proc = end_time - start_time_proc;
    double duration_proc = elapsed_time_proc.count() * 1e+3;

    // Show the success message on completion
    if( DEBUG && isRankRoot ) {
        std::cout << "SUCCESS: "
                  << "TF-IDF finished in time: " << duration_proc << " milliseconds"
                  << " (" << duration_proc / (60 * 1000) << " minutes)"
                  << ", Ranks: " << upcxx::rank_n()
                  << ", Files: " << file_count
                  << ", Files/Rank: " << base_files_per_rank
                  << ", Objects: " << inserted_objects << std::endl;
        std::cout << "benchmark,platform,nodes,ranks,files,dataset,"
                  << "processing time (ms),total time (ms),data load time (ms),data transfer time (ms)" << std::endl;
    }

    // Log the elapsed execution time in csv format
    if( LOG && isRankRoot ) {
        std::cout << "TF-IDF" << SEP
                  << "Saddlebag" << SEP
                  << (int) upcxx::rank_n() / ranks_per_node << SEP
                  << upcxx::rank_n() << SEP
                  << file_count << SEP
                  << getFileName(fileNames) << SEP
                  << duration_proc << SEP
                  << duration_total << SEP
                  << duration_data_gen << SEP
                  << duration_transfer
                  << std::endl;
    }

    upcxx::finalize();
    return 0;
}
