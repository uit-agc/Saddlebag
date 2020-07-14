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

#ifndef HASHF_CPP
#define HASHF_CPP

#include <string>
#include <vector>
#include "city.h"

//This file contains hash functions, based on CityHash 

int c_abs(int input)
{
	if(input >= 0)
		return input;
	return input*-1;
}

int hashf(std::string key)
{
	return c_abs(CityHash32((const char*)key.c_str(), (size_t)key.size()));
}


int hashf(int key)
{
	return c_abs(CityHash32((const char*)(&key), sizeof(key)));
}

int hashf(std::pair<int, int> key)
{
	int tmp = key.first + key.second;
	return c_abs(CityHash32((const char*)(&tmp), sizeof(tmp)));
}

int hashf(std::vector<std::string> key)
{
	std::string tmp = "";

	for (auto s : key)
	{  
		tmp += s;
	}
	return hashf(tmp);
}


#endif