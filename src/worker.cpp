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

#ifndef WORKER_CPP
#define WORKER_CPP

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <sstream>
#include <typeinfo>
#include <unordered_map>
#include <upcxx/upcxx.hpp>

#include "table.cpp"
#include "utils.hpp"

namespace saddlebags {

template<typename TableKey_T=uint8_t, typename ItemKey_T=unsigned int, typename Msg_T=double>
class Worker {

    public:

    int team_total_workers;
    int total_workers;
    int rank_n_;
    int total_nodes;
    int rank_me_;
    int team_rank_me_;
    int my_local_coord;
    int my_node_index;
    int total_tables = 0;
    int error = 0;

    SendingMode sending_mode = Combining;
    unsigned int replication_level = 0;
    unsigned int cycles_counter = 0;

    Worker(std::size_t buffer_size = INITIAL_RESERVE_SIZE, SendingMode mode = Combining) {
        BUFFER_MAX_SIZE = buffer_size;
        init_upcxx_variables();
        set_mode(mode);
        tables.reserve(5);
        create_buffers();
        create_buffers_gptr_init();
    }

    /**
     * Destructor: Release memory from buffers
     */
    ~Worker() {
        clear_buffers();
        destroy_buffers();
        destroy_items();
    }

     /**
      *
      * @param table_key
      * @param item_key
      * @return
      */
    inline std::size_t get_partition(const TableKey_T & table_key, const ItemKey_T & item_key) {
        // Using table and item key, find the right partition
        return distrib_hash(item_key) % total_workers;
    }

    /*******************************************
     *                                        *
     *                 TABLES                 *
     *                                        *
     ******************************************/

    /**
     * Add a new table to a worker, given an item class
     */
    template<template<typename, typename, typename> class ObjectType>
    void add_table(TableKey_T table_key, bool is_global = true) {
        auto table = new TableContainer<TableKey_T, ItemKey_T, Msg_T, ObjectType<TableKey_T, ItemKey_T, Msg_T>>();
        tables.push_back(table);
        assert(table_key == tables.size() - 1);

        tables[table_key]->is_global = is_global;
        tables[table_key]->myTableKey = table_key;
        tables[table_key]->worker = this;
        total_tables = tables.size();

        // TODO: How to reserve space for items?
        // tables[table_key]->mapped_items.reserve(INITIAL_RESERVE_SIZE);

        // TODO [Enhancement]: Support arbitrary Table ID. Simplify storing list of tables.
        //                     Right now value > 0 will cause error for the first table to be added.
    }

    /*******************************************
     *                                        *
     *              PUSH CYCLES               *
     *                                        *
     ******************************************/

    /**
     * Enqueue push request in outgoing buffers
     */
    void enqueue_push_request(Message<TableKey_T, ItemKey_T, Msg_T> const& msg) {
        // Using source and destination item, find the right buffer
        // int src_rank = get_partition(msg.src_table, msg.src_item);
        int dest_rank = get_partition(msg.dest_table, msg.dest_item);

        if (dest_rank < total_workers) {
            auto messages_total = get_messgaes_count_send(dest_rank);

            if (SADDLEBAG_DEBUG > 5 && messages_total == BUFFER_MAX_SIZE) {
                // ERROR: Out of space
                std::cout << "[Rank " << upcxx::rank_me() << "]"
                          << " Fatal Error: Out of space for buffers (currently set to " << BUFFER_MAX_SIZE << ")."
                          << " Increase the buffer size, and try again."
                          << std::endl;
                // assert(messages_total < BUFFER_MAX_SIZE);
            }

            if (messages_total >= BUFFER_MAX_SIZE) {
                *(my_push_buffers_size.at(dest_rank)) = messages_total + 1;
                return; // ERROR: Out of space
            }

            auto send_buffer = my_push_buffers.at(dest_rank);
            send_buffer[messages_total] = msg;
            *(my_push_buffers_size.at(dest_rank)) = messages_total + 1;

        } else {
            std::cout << "[Rank " << upcxx::rank_me() << "]"
                      << " Error: Item " << msg.dest_item << " has incorrect partition " << dest_rank << "."
                      << std::endl;
        }
    }

    /**
     *
     */
    void cycle(int iter = 1, bool do_work = true, bool do_comm = true){

        if (cycles_counter == 0) {
            create_buffers_gptr_wait();
        }

        for (int i = 0; i < iter; i++) {
#if DEBUG_TIME_MEASUREMENTS
            auto start_time = std::chrono::high_resolution_clock::now();
#endif
            // Let communication from previous cycle wrap-up
            upcxx::progress();
            upcxx::barrier();
            std::ostringstream s;

            if (SADDLEBAG_DEBUG > 5 && rank_me_ == rank_n_ - 1) {
                print_push_buffers();
            }

            if (do_comm) {
                if (is_local_root()) {
                    validate_buffer_space();
                }

                if (total_nodes == 1 && UPCXX_GPTR_LOCAL_ON) {
                    apply_push_incoming_local();
                } else {
                    apply_push_incoming_remote();
                }

                // Note values before buffers are cleared (prior to work)
                s << "Messages sent: " << messages_sent << ", recv (local): " << messages_recv_local << ", recv (remote): " << messages_recv_remote << ". "
                  << "Buffer size min: " << buffer_size_min << ", max: " << buffer_size_max << ", recommended: " << round_off(buffer_size_max) << ".";

                upcxx::barrier(); // Important for everyone to finish
                clear_buffers();
            }

            if (do_work) {
                work();
            }

            if (SADDLEBAG_DEBUG > 6 && rank_me_ == 0) {
                debug_tables_push(0);
            }

            if (SADDLEBAG_DEBUG > 6 && rank_me_ == rank_n_ - 1) {
                print_push_buffers();
            }

            if ((rank_me_ == 0 || (is_local_root()) && cycles_counter == 2) && SADDLEBAG_DEBUG) {
#if DEBUG_TIME_MEASUREMENTS
                auto end_time = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> elapsed_time_total = end_time - start_time;
                double time = elapsed_time_total.count();
#endif
                std::cout << "[Rank " << upcxx::rank_me() << "] "
#if DEBUG_TIME_MEASUREMENTS
                          << "[Iter " << cycles_counter << "] " << time << "s elapsed. "
#endif
                          << s.str()
                          << std::endl;
            }

            cycles_counter++;
        }
    }

    /*******************************************
     *                                        *
     *                 ITEMS                  *
     *                                        *
     ******************************************/

    /**
     * Insert an Item and return reference to it, so that it can be accessed by caller
     */
    template<template<typename, typename, typename> class ObjectType>
    ObjectType<TableKey_T, ItemKey_T, Msg_T>* add_item(
        TableKey_T table_key, ItemKey_T item_key, bool is_remote = false, bool is_create = true) {
        int status = 0;
        return add_item<ObjectType>(table_key, item_key, is_remote, is_create, status);
    }

    /**
     * Insert an Item and return reference to it, so that it can be accessed by caller
     */
    template<template<typename, typename, typename> class ObjectType>
    ObjectType<TableKey_T, ItemKey_T, Msg_T>* add_item(
        TableKey_T table_key, ItemKey_T item_key, bool is_remote, bool is_create, int& status) {
        const int CREATED_NEW_LOCAL = 100;
        const int REQUESTED_NEW_REMOTE = 200;
        const int FOUND_EXISTING_LOCAL = 300;
        const int IGNORED_NEW_REMOTE = 400;
        const int IGNORED_NEW_LOCAL = 500;
        const int NOT_FOUND = 0;
        status = NOT_FOUND;

        if (get_partition(table_key, item_key) == rank_me_) {
            assert(table_key < tables.size());
            auto target_table = tables[table_key];
            auto target_map = target_table->get_items();
            auto it = target_map->find(item_key);

            if (it == target_map->end()) {
                if (is_create) {
                    //create new object of ObjectType
                    auto new_obj = new ObjectType<TableKey_T, ItemKey_T, Msg_T>();
                    new_obj->myTableKey = table_key;
                    new_obj->myItemKey = item_key;
                    new_obj->worker = this;
                    new_obj->on_create();
                    new_obj->refresh();

#if ROBIN_HASH
                    target_map->insert(item_key, new_obj);
#else
                    target_map->insert({item_key, new_obj});
#endif

                    status = CREATED_NEW_LOCAL;
                    return new_obj;
                }

                status = IGNORED_NEW_LOCAL;
                return nullptr;
            }

            auto obj = reinterpret_cast<ObjectType<TableKey_T, ItemKey_T, Msg_T>*>((*it).second);
            obj->refresh();
            status = FOUND_EXISTING_LOCAL;
            return obj;
        }

        if (get_partition(table_key, item_key) != rank_me_ && is_create && is_remote) {
            Message<TableKey_T, ItemKey_T, Msg_T> msg;
            msg.dest_table = table_key;
            msg.dest_item = item_key;
            msg.src_table = table_key;
            msg.src_item = item_key;
            msg.value = Msg_T();

            enqueue_push_request(msg);
            status = REQUESTED_NEW_REMOTE;
            return nullptr;
        }

        status = IGNORED_NEW_REMOTE;
        return nullptr;
    }

    /**
     *
     */
    void inline set_mode(SendingMode mode = Combining) {
        sending_mode = mode;
    }

    /**
     * Return iterator to item-map in which every item is cast to derived item type
     */
    template<template<typename, typename, typename> class ObjectType>
    Robin_Map<ItemKey_T, ObjectType<TableKey_T, ItemKey_T, Msg_T>*> iterate_table(TableKey_T table_key) {
        assert(NULL); // TODO: Fix implementation

        // TableContainerBase<TableKey_T, ItemKey_T, Msg_T>* base_table = tables[table_key];
        // auto derived_table = reinterpret_cast<TableContainer<TableKey_T, ItemKey_T, Msg_T, ObjectType<TableKey_T, ItemKey_T, Msg_T>>*>(base_table);
        // return derived_table->mapped_items;
    }

    private:

    // We use a flat list for message buffers sent from this to each N process
    std::size_t BUFFER_MAX_SIZE = INITIAL_RESERVE_SIZE;
    std::vector< upcxx::dist_object<upcxx::global_ptr<std::size_t>>* > my_push_size_g;
    std::vector< upcxx::global_ptr<std::size_t> > their_push_size_g;

    std::vector<std::size_t*> my_push_buffers_size;
    std::vector<std::size_t*> their_local_push_size;
    std::vector<upcxx::future< upcxx::global_ptr<std::size_t> >> fetch_futures_size;

    std::vector< upcxx::dist_object<upcxx::global_ptr<Message<TableKey_T, ItemKey_T, Msg_T>>>* > my_push_buffers_g;
    std::vector< upcxx::global_ptr<Message<TableKey_T, ItemKey_T, Msg_T>> > their_push_buffers_g;

    std::vector< Message<TableKey_T, ItemKey_T, Msg_T>* > my_push_buffers;
    std::vector< Message<TableKey_T, ItemKey_T, Msg_T>* > their_local_push_buffers;
    std::vector<upcxx::future< upcxx::global_ptr<Message<TableKey_T, ItemKey_T, Msg_T>> >> fetch_futures_msgs;

    std::vector< upcxx::future<std::size_t> > rget_futures_size;
    std::vector< upcxx::future<> > rget_futures_msgs;
    std::vector< upcxx::global_ptr<std::size_t> > their_remote_push_size_g;
    std::vector< upcxx::global_ptr<Message<TableKey_T, ItemKey_T, Msg_T>> > their_remote_push_buffers_g;
    std::vector<std::size_t*> their_remote_push_size;
    std::vector< Message<TableKey_T, ItemKey_T, Msg_T>* > their_remote_push_buffers;

    std::vector<TableContainerBase<TableKey_T, ItemKey_T, Msg_T>*> tables;

    std::size_t messages_sent = 0;
    std::size_t messages_recv_local = 0;
    std::size_t messages_recv_remote = 0;
    std::size_t buffer_size_min = 0;
    std::size_t buffer_size_max = 0;

    const static int ERROR_OUT_OF_MEMORY = 1001;
    const static int ERROR_NOT_ENOUGH_BUFFER_SPACE = 1002;

    int N; // Total processes
    int W; // Total nodes
    int M; // Maximum number of messages between two processes

    std::vector<bool> proc_local;
    std::vector<std::size_t> rank_in_local;
    std::vector<std::size_t> rank_in_world;

    /**
     *
     */
    void init_upcxx_variables() {
        upcxx::team & local_team = upcxx::local_team();
        upcxx::team & world_team = upcxx::world();

        rank_me_ = upcxx::rank_me();
        team_rank_me_ = local_team.rank_me();
        rank_n_ = upcxx::rank_n();
        total_workers = upcxx::rank_n();
        team_total_workers = local_team.rank_n();
        total_nodes = (int) total_workers / team_total_workers;
        total_nodes += total_workers % team_total_workers == 0 ? 0 : 1;

        N = total_workers;
        W = total_nodes;
        M = BUFFER_MAX_SIZE;
        my_local_coord = -1;

        proc_local.reserve(total_workers);
        rank_in_local.reserve(total_workers);
        rank_in_world.reserve(total_workers);
        for (int i = 0; i < total_workers; i++) {
            proc_local.emplace_back(false);
            rank_in_local.emplace_back(-1);
            rank_in_world.emplace_back(-1);
        }

        for (int i = 0; i < total_workers; i++) {
            rank_in_local.at(i) = local_team.from_world(i, -1);
            proc_local.at(i) = upcxx::local_team_contains(i);
            if (SADDLEBAG_DEBUG > 4 && is_local_root() && proc_local.at(i)) {
                print_message("Process " + std::to_string(i) + " is local!");
            }
        }
    }

    /*******************************************
     *                                        *
     *                BUFFERS                 *
     *                                        *
     ******************************************/

    /**
     * Create global pointers and distributed objects for push buffers
     * Initialize buffers, including reserving space
     */
    void create_buffers() {
        const int size_msg_struct = sizeof(struct Message<TableKey_T, ItemKey_T, Msg_T>);
        std::string message = "";

        my_push_size_g.reserve(total_workers);
        their_push_size_g.reserve(total_workers);

        my_push_buffers_size.reserve(total_workers);
        their_local_push_size.reserve(total_workers);
        fetch_futures_size.reserve(total_workers);

        my_push_buffers_g.reserve(total_workers);
        their_push_buffers_g.reserve(total_workers);

        my_push_buffers.reserve(total_workers);
        their_local_push_buffers.reserve(total_workers);
        fetch_futures_msgs.reserve(total_workers);

        rget_futures_size.reserve(total_workers);
        rget_futures_msgs.reserve(total_workers);
        their_remote_push_size_g.reserve(total_workers);
        their_remote_push_buffers_g.reserve(total_workers);
        their_remote_push_size.reserve(total_workers);
        their_remote_push_buffers.reserve(total_workers);

        for (int i = 0; i < total_workers; i++) {
            auto my_ptr = upcxx::new_<std::size_t>(0);
            auto my_dist = new upcxx::dist_object<upcxx::global_ptr<std::size_t>>(my_ptr);

            my_push_buffers_size.emplace_back(my_ptr.local());
            my_push_size_g.push_back(my_dist);
            progress(i);
        }

        try {
            for (int i = 0; i < total_workers; i++) {
                auto my_ptr = upcxx::new_array<Message<TableKey_T, ItemKey_T, Msg_T>>(BUFFER_MAX_SIZE);
                auto my_dist = new upcxx::dist_object<upcxx::global_ptr<Message<TableKey_T, ItemKey_T, Msg_T>>>(my_ptr);

                my_push_buffers.emplace_back(my_ptr.local());
                my_push_buffers_g.emplace_back(my_dist);
                progress(i);
            }

            message += "Messages array: " + std::to_string(BUFFER_MAX_SIZE) + " (M: " + std::to_string(M) + ", ";
            message += "size of one message: " + std::to_string(size_msg_struct) + ")";
            if (rank_me_ == 0 && SADDLEBAG_DEBUG) {
                print_message(message);
            }

            // Message buffers for holding values received from upcxx::rget
            for (int i = 0; i < total_workers; i++) {
                if (is_process_local(i)) {
                    upcxx::global_ptr<std::size_t> size_g;
                    upcxx::global_ptr<Message<TableKey_T, ItemKey_T, Msg_T>> buffer_g;
                    their_remote_push_size_g.push_back(size_g);
                    their_remote_push_buffers_g.push_back(buffer_g);
                    their_remote_push_size.push_back(nullptr);
                    their_remote_push_buffers.push_back(nullptr);
                } else {
                    auto size_g = upcxx::new_<std::size_t>(0);
                    auto buffer_g = upcxx::new_array<Message<TableKey_T, ItemKey_T, Msg_T>>(BUFFER_MAX_SIZE);
                    their_remote_push_size_g.push_back(size_g);
                    their_remote_push_buffers_g.push_back(buffer_g);
                    their_remote_push_size.push_back(size_g.local());
                    their_remote_push_buffers.push_back(buffer_g.local());
                }
                progress(i);
            }
        } catch (std::bad_alloc& ba) {
            std::cout << "[Rank " << rank_me_ << "] "
                      << "FATAL ERROR: Out of memory with " << rank_n_
                      << " processes for buffer size of " << BUFFER_MAX_SIZE
                      << " (" << ba.what() << ")." << std::endl;
            error = ERROR_OUT_OF_MEMORY;
            exit(0);
        }
        // TODO [Enhancement]: Indicate estimated maximum buffer size possible
    }

    /**
     * Initialize buffers from other local and remote processes
     */
    void create_buffers_gptr_init() {
        upcxx::barrier();
        assert(my_push_size_g.size() == total_workers);
        assert(my_push_buffers_g.size() == total_workers);

        for (int i = 0; i < total_workers; i++) {
            fetch_futures_size.push_back(my_push_size_g.at(i)->fetch(i));
            fetch_futures_msgs.push_back(my_push_buffers_g.at(i)->fetch(i));
            progress(i);
        }
    }

    /**
     * Wait for completion of buffers from other local and remote processes
     */
    void create_buffers_gptr_wait() {
        // Important: Necessary for fetch() requests to be processed, without getting stuck
        // Alternatively, run some `std::cout << std::endl;` calls
        upcxx::barrier();

        for (int i = 0; i < total_workers; i++) {
            auto fut_sz = fetch_futures_size[i].wait();
            their_push_size_g.push_back(fut_sz);
            auto fut_msg = fetch_futures_msgs[i].wait();
            their_push_buffers_g.push_back(fut_msg);
            progress(i);
        }

        // Pointers to buffers in local processes
        for (int i = 0; i < total_workers; i++) {
            if (their_push_size_g[i].is_local()) {
                their_local_push_size.push_back(their_push_size_g[i].local());
            } else {
                their_local_push_size.push_back(nullptr);
            }

            if (their_push_buffers_g[i].is_local()) {
                their_local_push_buffers.push_back(their_push_buffers_g[i].local());
            } else {
                their_local_push_buffers.push_back(nullptr);
            }

            progress(i);
        }

        if (SADDLEBAG_DEBUG > 2 && rank_me_ == 0) {
            std::cout << "[Rank " << upcxx::rank_me() << "]"
                      << " Finished receive of " << total_workers << " fetch requests."
                      << " Total entries for buffers size: " << their_local_push_size.size() << "."
                      << " Total entries for buffers messages: " << their_local_push_buffers.size() << "."
                      << std::endl;
        }

        assert(their_local_push_size.size() == total_workers);
        assert(their_local_push_buffers.size() == total_workers);
        assert(their_remote_push_size_g.size() == total_workers);
        assert(their_remote_push_buffers_g.size() == total_workers);
        assert(their_remote_push_size.size() == total_workers);
        assert(their_remote_push_buffers.size() == total_workers);
    }

    /**
     * Clear buffers, between cycles
     */
    void clear_buffers() {
        messages_sent = 0;
        messages_recv_local = 0;
        messages_recv_remote = 0;
        buffer_size_min = 0;
        buffer_size_max = 0;

        for (int i = 0; i < total_workers; i++) {
            *(my_push_buffers_size.at(i)) = 0;

            if (!is_process_local(i)) {
                *(their_remote_push_size.at(i)) = 0;
            }
        }

        fetch_futures_size.clear();
        fetch_futures_msgs.clear();
        rget_futures_size.clear();
        rget_futures_msgs.clear();
    }

    /**
     * Delete and release memory from buffers
     */
    void destroy_buffers() {
        for (auto buf : their_remote_push_buffers) {
            if (buf != nullptr) {
                delete[] buf;
            }
        }

        // TODO: Delete respective to any new_ calls
        // // delete my_push_buffers_g;
        // // upcxx::delete_array(my_push_buffers_g);
        // Delete related to
        // // their_remote_push_size_g
        // // their_remote_push_buffers_g
    }

    /**
     * Delete and release memory for items
     *
     */
    void destroy_items() {
        for (auto table_iterator : tables) {
            table_iterator->destroy_items();
        }
    }

    /**
     *
     */
    void work() {
        for (auto table_iterator : tables) {
            for (auto obj_iterator : (*(table_iterator->get_items()))) {
                obj_iterator.second->before_work();
                obj_iterator.second->do_work();
                obj_iterator.second->finishing_work();
            }
        }
    }

    /**
     * Process incoming push requests from local processes
     */
    void apply_push_incoming_local() {
        for (int i = 0; i < total_workers; i++) {
            if (is_process_local(i)) {
                auto messages_total = valid_buffer_size(get_messgaes_count_recv(i));
                auto recv_buffer = their_local_push_buffers.at(i);
                if (messages_total > 0) {
                    messages_recv_local += process_push_buffer(recv_buffer, messages_total);
                }
                *(their_local_push_size.at(i)) = 0;
            }
            progress(i);
        }

        // How many messages I enqueued in my buffers?
        if (SADDLEBAG_DEBUG > 0 && rank_me_ == 0) {
            for (int i = 0; i < total_workers; i++) {
                messages_sent += valid_buffer_size(get_messgaes_count_send(i));
            }
        }

        if (SADDLEBAG_DEBUG > 4 && rank_me_ == 0) {
            std::cout << "[Rank " << upcxx::rank_me() << "] "
                      << "[Iter " << cycles_counter << "]"
                      << " Sent messages: " << messages_sent << ", "
                      << " Received messages (local): " << messages_recv_local
                      << std::endl;
        }
    }

    /**
     * Process incoming push requests from remote processes
     */
    void apply_push_incoming_remote() {
        std::size_t messages_total = 0;
        Message<TableKey_T, ItemKey_T, Msg_T>* recv_buffer = nullptr;

        // Step 1: Send out rget requests for buffers size
        for (int i = 0; i < total_workers; i++) {
            if (is_process_local(i)) {
                upcxx::future<std::size_t> fut;
                rget_futures_size.push_back(fut);
            } else {
                auto fut = upcxx::rget(their_push_size_g.at(i));
                rget_futures_size.push_back(fut);
            }
            progress(i);
        }
        assert(rget_futures_size.size() == total_workers);

        // Step 2: Meanwhile, process messages in my own buffer for myself
        messages_total = valid_buffer_size(get_messgaes_count_recv(rank_me_));
        recv_buffer = their_local_push_buffers.at(rank_me_);
        messages_recv_local += process_push_buffer(recv_buffer, messages_total);
        *(their_local_push_size.at(rank_me_)) = 0;

        // Step 3a: Wait for size values
        // Step 3b: Send out rget requests for buffers
        for (int i = 0; i < total_workers; i++) {
            if (is_process_local(i)) {
                upcxx::future<> fut;
                rget_futures_msgs.push_back(fut);
            } else {
                *(their_remote_push_size.at(i)) = rget_futures_size.at(i).wait();
                messages_total = valid_buffer_size(*(their_remote_push_size.at(i)));
                upcxx::future<> fut = upcxx::rget(their_push_buffers_g.at(i),
                            their_remote_push_buffers.at(i),
                            messages_total);
                rget_futures_msgs.push_back(fut);
                // TODO [Enhancement]: Use upcxx::then() to combine futures!
            }
            progress(i);
        }
        // TODO [Enhancement]: Use upcxx::when_all() to combine all futures!
        assert(rget_futures_msgs.size() == total_workers);

        // Step 4: Meanwhile, process messages from local processes
        apply_push_incoming_local();

        // Step 5: Process messages from remote processes
        for (int i = 0; i < total_workers; i++) {
            if (!is_process_local(i)) {
                rget_futures_msgs.at(i).wait();
                messages_total = valid_buffer_size(*(their_remote_push_size.at(i)));
                recv_buffer = their_remote_push_buffers.at(i);
                messages_recv_remote += process_push_buffer(recv_buffer, messages_total);
            }
            progress(i);
        }

        if (SADDLEBAG_DEBUG > 4 && rank_me_ == 0) {
            std::cout << "[Rank " << upcxx::rank_me() << "] "
                      << "[Iter " << cycles_counter << "]"
                      << " Received remote messages: " << messages_recv_remote
                      << std::endl;
        }
    }

    /**
     *
     */
    int process_push_buffer(Message<TableKey_T, ItemKey_T, Msg_T>* recv_buffer, std::size_t messages_total = 0) {
        messages_total = valid_buffer_size(messages_total);

        for (int i = 0; i < messages_total; i++) {
            auto msg = recv_buffer[i];
            tables[msg.dest_table]->apply_push_to_item(msg, !DEBUG_DISABLE_CREATE_ON_PUSH);
            progress(i);
        }

        return messages_total;
    }

    /**
     *
     */
    void print_push_buffers() {
        std::size_t msg_counter = 0;
        std::size_t error_msg_counter = 0;
        std::cout << "[Rank " << upcxx::rank_me() << "] "
                  << "[Iter " << cycles_counter << "]"
                  << " Buffers status ..." << std::endl;

        for (int i = 0; i < total_workers; i++) {

            if (is_process_local(i)) {
                auto recv_buffer = their_local_push_buffers.at(i);
                auto messages_total = valid_buffer_size(*(their_local_push_size.at(i)));

                for (int k = 0; k < messages_total; k++) {
                    auto msg = recv_buffer[k];

                    if (msg.dest_table >= tables.size()) {
                        if (SADDLEBAG_DEBUG > 5) {
                            std::cout << "[Rank " << upcxx::rank_me() << "]"
                                      << " Error: Malformed message with table: " << msg.dest_table
                                      << " , item: " << msg.dest_item << "."
                                      << std::endl;
                        }
                        error_msg_counter++;
                        continue;
                    }

                    std::cout << msg.src_item << "->" << msg.dest_item << " (" << msg.value << ")\t";
                    msg_counter++;
                }
            }
            std::cout << std::endl;
        }

        if (SADDLEBAG_DEBUG > 1 && error_msg_counter > 0) {
            std::cout << "[Rank " << upcxx::rank_me() << "]"
                      << " Error: Malformed messages: " << error_msg_counter
                      << " , Correct Messages: " << msg_counter << "."
                      << std::endl;
        }
    }

    /**
     *
     */
    void debug_tables_push(TableKey_T table_key) {
        assert(table_key < tables.size());
        Message<TableKey_T, ItemKey_T, Msg_T> msg;
        msg.dest_table = table_key;
        msg.dest_item = ItemKey_T();
        msg.src_table = table_key;
        msg.src_item = ItemKey_T();
        msg.value = Msg_T();
        std::size_t msg_counter = 0;

        for (auto obj_iterator : (*(tables[table_key]->get_items()))) {
            auto obj = obj_iterator.second;
            msg.dest_item = obj->myItemKey;
            tables[table_key]->apply_push_to_item(msg);
            msg_counter++;
        }

        for (int i = 1000; i < total_workers; i++) {
            msg.dest_item = 47 * total_workers + i;
            tables[table_key]->apply_push_to_item(msg);
            msg_counter++;
        }

        std::cout << "[Rank " << upcxx::rank_me() << "]"
                  << " Tested dummy messages: " << msg_counter
                  << " (Cycle " << cycles_counter << ")."
                  << std::endl;

        // TODO: Test stressing filling push buffers
    }

    /**
     *
     */
    inline bool is_process_local(int rank) {
        if (UPCXX_GPTR_LOCAL_ON &&
            rank < their_local_push_buffers.size() &&
            rank < their_local_push_size.size()) {
            return their_local_push_buffers[rank] != nullptr &&
                   their_local_push_size[rank] != nullptr;
        }
        return false;
    }

    /*
     *
     */
    inline TableContainerBase<TableKey_T, ItemKey_T, Msg_T>* get_table(TableKey_T table_key) {
        // TODO: Fix type conversion from base
        assert(table_key < tables.size());
        return tables[table_key];
    }

    /**
     *
     * @param dest_rank
     * @return
     */
     inline std::size_t get_messgaes_count_send(int dest_rank) {
         assert(dest_rank < my_push_buffers_size.size());
         return *(my_push_buffers_size.at(dest_rank));
     }

    /**
     *
     * @param src_rank
     * @return
     */
    inline std::size_t get_messgaes_count_recv(int src_rank) {
        assert(src_rank < their_local_push_size.size());
        return *(their_local_push_size.at(src_rank));
    }

    /**
     *
     * @param size
     * @return
     */
    inline std::size_t valid_buffer_size(std::size_t size) {
        size = (size <= BUFFER_MAX_SIZE) ? size : BUFFER_MAX_SIZE;
        return size;
    }

    /**
     * Validate push buffers for exceeding space
     */
    void validate_buffer_space() {
        std::size_t max = get_messgaes_count_send(rank_me_);
        std::size_t min = get_messgaes_count_send(rank_me_);

        // Check my send bufers
        for (int i = 0; i < total_workers; i++) {
            if (get_messgaes_count_send(i) > max) {
                max = get_messgaes_count_send(i);
            }

            if (get_messgaes_count_send(i) < min) {
                min = get_messgaes_count_send(i);
            }
        }

        // Check my receive bufers
        for (int i = 0; i < total_workers; i++) {
            if (is_process_local(i)) {
                if (get_messgaes_count_recv(i) > max) {
                    max = get_messgaes_count_recv(i);
                }

                if (get_messgaes_count_recv(i) < min) {
                    min = get_messgaes_count_recv(i);
                }
            }
        }
        // TODO: Check all send/recv buffers of all local processes
        //       Need to get global pointers for other buffers (not destined to myself)

        buffer_size_min = min;
        buffer_size_max = max;

        if (max > BUFFER_MAX_SIZE) {
            // Round off max for readability
            max = round_off(max, true, true);

            std::ostringstream s;
            s << "FATAL ERROR: Out of space, needed " << max << " (currenty set to " << BUFFER_MAX_SIZE << ").";
            auto prev_error = error;
            error = ERROR_NOT_ENOUGH_BUFFER_SPACE;

            if (prev_error == 0 && rank_me_ == 0) {
                print_message(s.str());
            }
        }
        // TODO [Enhancement]: Dynamically resize global_ptr, copying values over to new array
        //                     May require fetching global_ptr reference in each cycle!
     }

    /**
     *
     * @param s
     */
    inline void print_message(std::string s) {
        std::cout << "[Rank " << rank_me_ << "] "
                  << "[Iter " << cycles_counter << "] "
                  << s
                  << std::endl;
    }

    /**
     *
     * @return
     */
    inline bool is_local_root() {
        return team_rank_me_ == 0;
    }

    /**
     *
     * @param max
     * @param is_K
     * @param is_M
     * @return
     */
    inline std::size_t round_off(std::size_t max, bool is_K = true, bool is_M = false) {
        if (max > 1e+6 && is_M) {
            max += 1e+6 - (max % (std::size_t) 1e+6);
        } else if (max > 1e+3 && is_K) {
            max += 1e+3 - (max % (std::size_t) 1e+3);
        }
        return max;
    }
};

} //end namespace

namespace upcxx {
template<class TableKey_T, class ItemKey_T, class Msg_T>
struct is_definitely_trivially_serializable<saddlebags::Message<TableKey_T, ItemKey_T, Msg_T>> : std::true_type {};

template<class TableKey_T, class ItemKey_T, class Msg_T>
struct is_definitely_serializable<std::vector<saddlebags::Message<TableKey_T, ItemKey_T, Msg_T>>> : std::true_type {};
}

#endif
