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

#include "hashf.cpp"

//Hash map implementation

//#define ROBIN_SWAPPING

namespace saddlebags
{

int bit_modulo(int x, int N) {
    return (x & (N-1));
}

template<typename keyT, typename valueT>
class Entry {
    public:
    int hash = -1;
    keyT first;
    valueT second;
};

template<typename keyT, typename valueT> class RobinIterator;

template<typename keyT, typename valueT>
class Robin_Map {
    public:

    using iterator = RobinIterator<keyT, valueT>;

    int size = 1024;
    Entry<keyT, valueT>* entries;
    const float load_factor = 0.5;
    int num_items = 0;

    Robin_Map() {
        entries = new Entry<keyT, valueT>[size];
        for(int i = 0; i < size; i++)
        {
            Entry<keyT, valueT> tmp;
            tmp.hash = -1;
            entries[i] = tmp;
        }
    }

    iterator begin(){
        for(int i = 0; i < size; i++)
        {
            if(entries[i].hash != -1)
            {
                return iterator(*this, i);
            }
        }
        return iterator(*this, size);
    }

    iterator end(){
        return iterator(*this, size);
    }

    int get_offset(Entry<keyT, valueT> entry, int location)
    {
        int desired_loc = bit_modulo(entry.hash, size);

        if(location >= desired_loc)
            return location - desired_loc;
        return location + (size-desired_loc);
    }

    int get_offset(Entry<keyT, valueT> entry, int location, int new_size)
    {
        int desired_loc = bit_modulo(entry.hash, new_size);

        if(location >= desired_loc)
            return location - desired_loc;
        return location + (new_size-desired_loc);
    }

    bool core_insert_with_hash(keyT key, valueT val, int hashed)
    {
        int location = bit_modulo(hashed, size);

        keyT key_to_place = key;
        valueT val_to_place = val;
        #ifdef OFFSET_LIMIT
        #endif
        int i = 0;
        while(true)
        {
            location = bit_modulo((location+i), size);

            if(entries[location].hash == -1)
            {
                entries[location].first = key_to_place;
                entries[location].second = val_to_place;
                entries[location].hash = hashed;
                return true;
            }
            #ifdef ROBIN_SWAPPING
            int offset = get_offset(entries[location], location);
            if(offset < i)
            {
                //Robin Hood swap
                std::swap(entries[location].first, key_to_place);
                std::swap(entries[location].second, val_to_place);
                std::swap(entries[location].hash, hashed);
                i = offset;
            }
            #endif
            i++;
        }
        return false;
    }


    bool core_insert(keyT key, valueT val)
    {
        int hashed = hashf(key);
        return core_insert_with_hash(key,val,hashed);
    }

    bool insert_new_array(keyT key, valueT val, int hashed, Entry<keyT, valueT>* entry_array, int new_size)
    {
        
        int location = bit_modulo(hashed, new_size);

        keyT key_to_place = key;
        valueT val_to_place = val;
        int i = 0;
        while(true)
        {
            location = bit_modulo((location+i), new_size);

            if(entry_array[location].hash == -1)
            {
                entry_array[location].first = key_to_place;
                entry_array[location].second = val_to_place;
                entry_array[location].hash = hashed;
                return true;
            }
            #ifdef ROBIN_SWAPPING
            int offset = get_offset(entry_array[location], location, new_size);
            if(offset < i)
            {
                //Robin Hood swap
                std::swap(entry_array[location].first, key_to_place);
                std::swap(entry_array[location].second, val_to_place);
                std::swap(entry_array[location].hash, hashed);
                i = offset;
            }
            #endif
            i++;
        }
        return false;
    }

    bool above_load_factor()
    {
        if(num_items > (float)size * load_factor)
        {
            return true;
        }
        return false;
    }

    iterator find(keyT key)
    {
        int hashed = hashf(key);
        int location = bit_modulo(hashed, size);

        int i = 0;
        while(true)
        {
            location = bit_modulo((location+i), size);


            if(entries[location].hash == -1)
            {
                return end();
            }

            if(entries[location].first == key)
            {
                return iterator(*this, location);
            }

            i++;
        }

        return end();
    }


    void insert(keyT key, valueT val)
    {
        if(above_load_factor())
        {
            expand(size*2);
        }

        if(core_insert(key, val) == true)
        {
            num_items += 1;
            return;
        }

    }

    void insert(keyT key, valueT val, int hashed)
    {
        if(above_load_factor())
        {
            expand(size*2);
        }

        if(core_insert_with_hash(key, val, hashed) == true)
        {
            num_items += 1;
            return;
        }

    }





    void expand(int new_size)
    {
        Entry<keyT, valueT>* new_entries = new Entry<keyT, valueT>[new_size];


        for(int i = 0; i < new_size; i++)
        {
            Entry<keyT, valueT> tmp;
            tmp.hash = -1;
            new_entries[i] = tmp;
        }
        
        for(int i = 0; i<size; i++)
        {
            if(entries[i].hash != -1)
            {

                insert_new_array(entries[i].first, entries[i].second, entries[i].hash, new_entries, new_size);
            }
        }
        delete[] entries;
        entries = new_entries;
        size = new_size;
    }

};

template<typename keyT, typename valueT>
class RobinIterator
{
    public:
    using value_type = valueT;
    using difference_type = std::ptrdiff_t;
    using pointer = valueT*;
    using reference = valueT&;
    using iterator = RobinIterator<keyT, valueT>;

    int current_loc = 0;
    Robin_Map<keyT, valueT> &my_map;
    keyT first;
    valueT second;

    RobinIterator(Robin_Map<keyT, valueT>& map) : my_map(map), current_loc(0)
    {}

    RobinIterator(Robin_Map<keyT, valueT>& map, int start_loc) : my_map(map), current_loc(start_loc)
    {}

    iterator & operator++()
    {
        do {
            current_loc+=1;
        } while(my_map.entries[current_loc].hash == -1 && current_loc != my_map.size);
        return *this;
    }

    bool operator==(const iterator& other)
    {
        return (current_loc == other.current_loc);
    }

    bool operator!=(const iterator& other)
    {
        return (current_loc != other.current_loc);
    }

    Entry<keyT, valueT> & operator*() const
    {
        return my_map.entries[current_loc];
    }
};



} //end namespace