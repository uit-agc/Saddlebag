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

//SNIPPET
#include <iostream>
#include <upcxx/upcxx.hpp>
#include <unistd.h>
#include <sched.h>

#include "saddlebags.hpp"

#define HELLO_TABLE 0

// we will assume this is always used in all examples
using namespace std;

template<class T, class D>
class Rect {
public:
    T id;
    D length;
    D width;

    T area() {
        return length * width;
    }
};

template<class Tk, class Ok, class Mt>
class Hello : public saddlebags::Item<Tk, Ok, Mt> {

    // friend class upcxx::access;

public:

    int id;

    void on_create() override {
    }

    //For each cycle
    void do_work() override {
        std::cout << "[Rank " << saddlebags::rank_me() << "] Hello World!" << std::endl;
    }

    void foreign_push(Mt val) override {
    }

    Mt foreign_pull(int tag) override {
        return upcxx::rank_me();
    }

    void refresh() override {
    }

    void returning_pull(saddlebags::Message<Tk, Ok, Mt> returning_message) override {
    }

    void finishing_work() override {
    }
};

namespace upcxx {
  template<class T, class D>
  struct is_definitely_trivially_serializable<Rect<T, D>> : std::true_type {};

  template<class Tk, class Ok, class Mt>
  struct is_definitely_trivially_serializable<Hello<Tk, Ok, Mt>> : std::true_type {};
}

using myRect = Rect<int,int>;

int main(int argc, char *argv[])
{
    // setup UPC++ runtime
    saddlebags::init();
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    int cpu = -1;
    cpu = sched_getcpu();

    cout << "[Rank " << upcxx::rank_me() << "]"
       << " Hello world from process " << upcxx::rank_me()
       << " out of " << upcxx::rank_n() << " processes,"
       << " on host "<< hostname
       << " (core " << cpu << ")"  <<endl;

    Rect<int, double> *r;
    r = new Rect<int, double>();
    r->length = 10;
    r->width = 10;
    double area = r->area();
    cout << area << endl;

    upcxx::rpc(0,[](int from, myRect r) {
       std::cout << from << ": " << r.id << "," << r.length << "," << r.width << std::endl;
      }, upcxx::rank_me(), myRect{1,2,3}).wait();

    auto worker = saddlebags::create_worker<int, int, int>();
    saddlebags::add_table<Hello>(worker, HELLO_TABLE, true);
    // Given a distributor type
    // saddlebags::add_table<saddlebags::CyclicDistributor, Hello>(worker, HELLO_TABLE, true);

    saddlebags::cycle(worker, false);
    saddlebags::cycle(worker, true);

    // close down UPC++ runtime
    saddlebags::finalize();
    return 0;
} 
//SNIPPET
