# Copyright 2019 Saddlebag Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This Makefile demonstrates the recommended way to build simple UPC++ programs.
# Note this uses some GNU make extensions for conciseness.
#
# To use this makefile, set the UPCXX_INSTALL variable to the upcxx install directory, e.g.
# make UPCXX_INSTALL=<myinstalldir> hello-world
# or (for bash)
# export UPCXX_INSTALL=<myinstalldir>; make hello-world

ifeq ($(wildcard $(UPCXX_INSTALL)/bin/upcxx-meta),)
$(error Please set UPCXX_INSTALL=/path/to/upcxx/install)
endif

UPCXX_THREADMODE ?= seq
UPCXX_GASNET_CONDUIT = ibv # smp -or- ibv
CC ?= mpicc
CCX ?= mpicxx
UPCXX_CODEMODE ?= O3
#UPCXX_CODEMODE ?= debug # Uncomment to debug

ENV = env UPCXX_THREADMODE=$(UPCXX_THREADMODE)

CXX = $(shell $(ENV) $(UPCXX_INSTALL)/bin/upcxx-meta CXX)
CPPFLAGS = $(shell $(ENV) $(UPCXX_INSTALL)/bin/upcxx-meta CPPFLAGS)
CXXFLAGS = $(shell $(ENV) $(UPCXX_INSTALL)/bin/upcxx-meta CXXFLAGS)
LDFLAGS = $(shell $(ENV) $(UPCXX_INSTALL)/bin/upcxx-meta LDFLAGS)
LIBS = $(shell $(ENV) $(UPCXX_INSTALL)/bin/upcxx-meta LIBS)

PTHREAD_FLAGS = -pthread
OPENMP_FLAGS = -fopenmp

# Be sure that all commands used to build one executable consistently pass either 
# a -O option to select the optimized/production version of UPC++ (for performance runs), 
# or a -g option to select the debugging version of UPC++ (for tracking down bugs in your application).
EXTRA_FLAGS = -O3
#EXTRA_FLAGS = -g  # Uncomment to debug

# Saddlebag specific parameters
BASE_DIR = ..
SRC_DIR = $(BASE_DIR)/src
LIB_DIR = $(BASE_DIR)/lib
INCLUDE_DIR = $(SRC_DIR)

CITY_HASH_INCL = $(LIB_DIR)/cityhash/src 
ALLOCATOR_INCL = $(LIB_DIR)/allocator 
XXHASH_INCL = $(LIB_DIR)/xxHash
# Add if needed $(CITY_HASH_INCL) $(ALLOCATOR_INCL)
INCLUDE_LIST = $(INCLUDE_DIR) $(XXHASH_INCL) $(CITY_HASH_INCL)
INC_FLAGS := $(addprefix -I,$(INCLUDE_LIST))

CITY_HASH_LIB = $(LIB_DIR)/cityhash/src/city.o
XXHASH_LIB = $(LIB_DIR)/xxHash/xxhash.o
# Add if needed -lm $(CITY_HASH_LIB)
SBC_CPP_FLAGS ?= $(INC_FLAGS) -MMD -MP -lm $(CITY_HASH_LIB)

# Programs to build, assuming each has a corresponding *.cpp file
#EXAMPLES = \
#	calculate-area \
#	hello-world \
#	pagerank \
#	sgd \
#	tfidf \
#	spmv \
#	spmv-upcxx

EXAMPLES = \
	hello-world \
	pagerank

# EXAMPLES = pagerank

all: $(EXAMPLES)

# The rule for building any example.
%: %.cpp $(wildcard *.h) $(wildcard *.hpp)
	(cd ../lib/xxHash && $(MAKE))
	$(CXX) $@.cpp $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS) $(LIBS) $(SBC_CPP_FLAGS) $(EXTRA_FLAGS) -o $@

clean:
	rm -f $(EXAMPLES)

.PHONY: clean all
