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

#include <iostream>
#include <sched.h>
#include <unistd.h>
#include <upcxx/upcxx.hpp>
#include "saddlebags.hpp"

#define HELLO_TABLE 0
#define DEBUG true

// we will assume this is always used in all examples
using namespace std;

template<class Tk = int, class Ok = int, class Mt = int>
class Hello : public saddlebags::Item<Tk, Ok, Mt> {

    public:

    int id = -1;
    int recvd = -1;
    using saddlebags::Item<Tk, Ok, Mt>::myItemKey;

    void on_create() override {
    }

    //For each cycle
    void do_work() override {

        if (this->worker->cycles_counter > 0 && recvd > 0 && DEBUG) {
            std::cout << "[Rank " << saddlebags::rank_me() << "]"
                      << " Cycle: " << this->worker->cycles_counter << "."
                      << " Hello World from Item " << myItemKey << "/" << id << "."
                      << " Received value " << recvd << "."
                      << std::endl;
        }

        for (int j = 0; j < 5; j++) {
            for (int i = 0; i < upcxx::rank_n(); i++) {
                this->push(HELLO_TABLE, get_key(i), id);
                this->push(HELLO_TABLE, get_key(i, 2), id);
                this->push(HELLO_TABLE, get_key(i, 4), id);
            }
        }

        int value = saddlebags::rank_me() == 0 ? 999 : saddlebags::rank_me() * 100;
        int target = (saddlebags::rank_me() + this->worker->cycles_counter + 1) % saddlebags::rank_n();
        this->push(HELLO_TABLE, get_key(target), value);
    }

    inline int get_key(int i, int p = 0) {
        return p * upcxx::rank_n() + i;
    }

    void on_push_recv(Mt val) override {
        this->recvd = val;
    }

    Mt foreign_pull(int tag) override {
        return upcxx::rank_me();
    }

    void refresh() override {
    }

    void returning_pull(saddlebags::Message<Tk, Ok, Mt> const& returning_message) override {
    }

    void finishing_work() override {
    }
};

template class Hello<int, int, int>;

namespace upcxx {
template<class Tk, class Ok, class Mt>
struct is_definitely_trivially_serializable<Hello<Tk, Ok, Mt>> : std::true_type {};
}

int main(int argc, char *argv[])
{
    int buffer_size = 500;
    if (argc > 1) {
        buffer_size = atoi(argv[1]);
    }

    // Setup UPC++ runtime
    saddlebags::init();
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    int cpu = -1;
    cpu = sched_getcpu();

    if (DEBUG) {
        cout << "[Rank " << upcxx::rank_me() << "]"
             << " Hello world from process " << upcxx::rank_me()
             << " out of " << upcxx::rank_n() << " processes,"
             << " on host " << hostname
             << " (core " << cpu << ")" << endl;
    }

    if (SADDLEBAG_DEBUG > 5) {

        int N = 10;
        int BUFFER_MAX_SIZE = 20;

        upcxx::global_ptr<upcxx::global_ptr<int>> g_m = upcxx::new_array<upcxx::global_ptr<int>>(N);
        for (int i = 0; i < N; i++) {
            auto g_i = upcxx::new_array<int>(BUFFER_MAX_SIZE);
            // g_m[i] = g_i; // not this way
            *(g_m.local() + i) = g_i;
        }

        upcxx::dist_object<upcxx::global_ptr<unsigned int>> *arr[10];
        std::vector < upcxx::dist_object<upcxx::global_ptr<unsigned int>>* > vec;

        for (int i = 0; i < 10; i++) {
            auto my_ptr = upcxx::new_<unsigned int>(i);
            auto my_dist = new upcxx::dist_object<upcxx::global_ptr<unsigned int>>(my_ptr);

            vec.push_back(my_dist);
            arr[i] = my_dist;
        }

        upcxx::barrier();
        for (int i = 0; i < 10; i++) {
            auto fut1 = vec[i]->fetch(0);
            auto fut2 = arr[i]->fetch(0);

            // when testing on single node
            std::cout << *(fut1.wait().local()) << std::endl;
            std::cout << *(fut2.wait().local()) << std::endl;
        }

        // upcxx::dist_object<upcxx::global_ptr<unsigned int>> arr1[10]; // won't compile
        upcxx::dist_object<upcxx::global_ptr<unsigned int>> *arr_dist;
        // arr_dist = new upcxx::dist_object<upcxx::global_ptr<unsigned int>>[10]; // error: use of deleted function
        std::vector<upcxx::dist_object<upcxx::global_ptr<unsigned int>>> vec_dist;

        for (int i = 0; i < 10; i++) {
            upcxx::dist_object<upcxx::global_ptr<unsigned int>> u_g(upcxx::new_<unsigned int>(i));
            // vec_dist.push_back(u_g); // error: use of deleted function
            // arr_dist[i] = u_g; // won't compile
        }
    }


    // TODO: `create_worker<unsigned char, unsigned char, unsigned char>` doesn't work well!
    // TODO: For instance, `myItemKey` isn't set correctly!
    auto worker = saddlebags::create_worker<int, int, int>(500);
    worker->add_table<Hello>(HELLO_TABLE);

    int my_id = 2 * upcxx::rank_n() + upcxx::rank_me();
    auto obj = worker->add_item<Hello>(HELLO_TABLE, my_id);
    if (obj != nullptr) {
        obj->id = my_id;
        obj->recvd = -999;
    }

    if (DEBUG) {
        std::cout << "[Rank " << worker->rank_me_ << "]"
                  << " Hello world from process " << worker->rank_me_
                  << " out of " <<  worker->rank_n_ << " processes,"
                  << " in local team process " << worker->team_rank_me_
                  << " of " << worker->team_total_workers << ","
                  << " on total " << worker->total_nodes << " nodes."
                  << " Inserted id " << (!obj ? -1 : obj->id) << "."
                  << std::endl;
    }

    worker->cycle();
    worker->cycle(2, false, false);

    // Close down UPC++ runtime
    saddlebags::destroy_worker(worker);
    saddlebags::finalize();
    return 0;
}
