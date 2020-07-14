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

#ifndef SADDLEBAGS_CPP
#define SADDLEBAGS_CPP

#include <upcxx/upcxx.hpp>
#include <unordered_map>
#include <iostream>
#include <typeinfo>

#include "utils.hpp"
#include "worker.cpp"

namespace saddlebags
{

upcxx::team & my_team = upcxx::local_team();

/*
 * Create new worker with a particular sending mode
 */
template<class key_T, class value_T, class message_T>
Worker<key_T, value_T, message_T>* create_worker(unsigned int size = INITIAL_RESERVE_SIZE, SendingMode mode = Combining) {
    return new Worker<key_T, value_T, message_T>(size, mode);
}

/*
 * Destroy worker
 */
template<class key_T, class value_T, class message_T>
Worker<key_T, value_T, message_T>* destroy_worker(Worker<key_T, value_T, message_T>* w) {
    // TODO: Delete Worker object and all its contents
    // delete w;
}

} //end namespace
#endif
