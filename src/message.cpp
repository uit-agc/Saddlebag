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

#ifndef MESSAGE_HPP
#define MESSAGE_HPP

namespace saddlebags
{

template<typename TableKey_T=uint8_t, typename ItemKey_T=unsigned int, typename Msg_T=double>
class Message {

  public:
    Msg_T value;
    TableKey_T src_table;
    TableKey_T dest_table;
    ItemKey_T dest_item;
    ItemKey_T src_item;

    /**
     *
     * @param msg
     * @param item
     * @param table
     */
    // Message(Msg_T msg, ItemKey_T item = ItemKey_T(), TableKey_T table = TableKey_T()) {
    //    dest_table = table;
    //    dest_item = item;
    //    value = msg;
    // }
    // TODO: Constructor with default values
};

} //end namespace

#endif
