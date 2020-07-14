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

#ifndef TABLECONTAINER_H
#define TABLECONTAINER_H

#include <assert.h>
#include <cmath>
#include <functional>
#include <iostream>
#include <unordered_map>

#include "item.cpp"
#include "hash_map.cpp"
#include "utils.hpp"

namespace saddlebags
{

template<typename TableKey_T, typename ItemKey_T, typename Msg_T> class Worker;

template <typename TableKey_T, typename ItemKey_T, typename Msg_T>
class TableContainerBase
{
    public:
    TableKey_T myTableKey;
    bool is_global = false;
    Worker<TableKey_T, ItemKey_T, Msg_T>* worker;
    Msg_T broadcast_value;
    ItemKey_T broadcast_origin_item;
    bool broadcast_enabled = false;


#if ROBIN_HASH
    virtual Robin_Map<ItemKey_T, Item<TableKey_T, ItemKey_T, Msg_T>*>* get_items() = 0;
#else
    virtual std::unordered_map<ItemKey_T, Item<TableKey_T, ItemKey_T, Msg_T>*>* get_items() = 0;
#endif

    virtual Item<TableKey_T, ItemKey_T, Msg_T>* create_new_item(ItemKey_T key) = 0;
    virtual int apply_push_to_item(Message<TableKey_T, ItemKey_T, Msg_T> const& msg) = 0;
    virtual int apply_push_to_item(Message<TableKey_T, ItemKey_T, Msg_T> const& msg, bool is_create) = 0;
    virtual void destroy_items() = 0;
};

template <typename TableKey_T, typename ItemKey_T, typename Msg_T, typename ItemType>
class TableContainer : public TableContainerBase<TableKey_T, ItemKey_T, Msg_T> {
    public:

#if ROBIN_HASH
    Robin_Map<ItemKey_T, ItemType*> mapped_items;
    Robin_Map<ItemKey_T, ItemType*> replicated_items;

    Robin_Map<ItemKey_T, Item<TableKey_T, ItemKey_T, Msg_T>*>* get_items() override {
        return reinterpret_cast<Robin_Map<ItemKey_T, Item<TableKey_T, ItemKey_T, Msg_T>*>*>(&mapped_items);
    }
#else
    std::unordered_map<ItemKey_T, ItemType*> mapped_items;
    std::unordered_map<ItemKey_T, ItemType*> replicated_items;

    std::unordered_map<ItemKey_T, Item<TableKey_T, ItemKey_T, Msg_T>*>* get_items() override {
        return reinterpret_cast<std::unordered_map<ItemKey_T, Item<TableKey_T, ItemKey_T, Msg_T>*>*>(&mapped_items);
    }
#endif

    /*
     *
     */
    ItemType* create_new_item(ItemKey_T key) {
        auto newobj = new ItemType();
        newobj->worker = this->worker;
        newobj->myItemKey = key;
        newobj->myTableKey = this->myTableKey;
        newobj->on_create();
        newobj->refresh();
        return newobj;
    }

    /**
     *
     * @param msg
     * @param is_create
     * @return
     */
    int apply_push_to_item(Message<TableKey_T, ItemKey_T, Msg_T> const& msg) {
        apply_push_to_item(msg, true);
    }

    /**
     *
     * @param msg
     * @param is_create
     * @return
     */
    int apply_push_to_item(Message<TableKey_T, ItemKey_T, Msg_T> const& msg, bool is_create) {
        const int CREATED_NEW_LOCAL = 100;
        const int REQUESTED_NEW_REMOTE = 200;
        const int FOUND_EXISTING_LOCAL = 300;
        const int IGNORED_NEW_REMOTE = 400;
        const int IGNORED_NEW_LOCAL = 500;
        const int NOT_FOUND = 0;
        int status = NOT_FOUND;

        // This check not necessary
        if (this->worker->get_partition(this->myTableKey, msg.dest_item) != upcxx::rank_me()) {
            status = IGNORED_NEW_REMOTE;
            return status;
        }

        ItemKey_T key = msg.dest_item;
        auto iterator = mapped_items.find(key);
        if(iterator == mapped_items.end()) {
            if (is_create) {
                auto newobj = create_new_item(key);
                newobj->on_push_recv(msg.value);
                status = CREATED_NEW_LOCAL;

#if ROBIN_HASH
                mapped_items.insert(key, newobj);
#else
                mapped_items[key] = newobj;
#endif
            } else {
                status = IGNORED_NEW_LOCAL;
            }
        } else {
            assert((*iterator).second != NULL);
            auto obj = (*iterator).second;
            obj->on_push_recv(msg.value);
            status = FOUND_EXISTING_LOCAL;
        }

        return status;
    }

    /*
     *
     */
    void destroy_items() {
        // TODO: Stored in target_table->get_items();
    }
};


// TODO: Return iterator to item-map in which every item is cast to derived item type
//template<template<class TableKey_T, class ItemKey_T, class Msg_Type> class ItemType, class TableKey_T, class ItemKey_T, class Msg_Type>
//Robin_Map<ItemKey_T, ItemType<TableKey_T, ItemKey_T, Msg_Type>*>
//iterate_table(TableKey_T table_key)
//{
//    TableContainerBase<TableKey_T, ItemKey_T, Msg_Type>* base_table = this->worker->tables[table_key];
//    auto derived_table = reinterpret_cast<TableContainer<TableKey_T, ItemKey_T, Msg_Type, ItemType<TableKey_T, ItemKey_T, Msg_Type>>*>(base_table);
//    return derived_table->mapped_items;
//}

}//end namespace

namespace upcxx {
  template<typename TableKey_T, typename ItemKey_T, typename Msg_T>
  struct is_definitely_trivially_serializable<saddlebags::Worker<TableKey_T, ItemKey_T, Msg_T>> : std::true_type {};

  template <typename TableKey_T, typename ItemKey_T, typename Msg_T>
  struct is_definitely_trivially_serializable<saddlebags::TableContainerBase<TableKey_T, ItemKey_T, Msg_T>> : std::true_type {};
}

#endif