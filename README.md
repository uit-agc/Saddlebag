Saddlebags
=======
Saddlebags is a high performance computing framework, created at the University of Tromsø. It is implemented in C++ with UPC++, and is inspired by big data systems and graph analytics frameworks.

Comprehensive information about Saddlebags can be found in the `doc` directory of this repository.

<!-- MarkdownTOC levels="2,3,4,5" autoanchor=false autolink=true -->

- [Requirements](#requirements)
- [Source Code for Saddlebags](#source-code-for-saddlebags)
    - [Source Code Organisation](#source-code-organisation)
- [How to Set Up](#how-to-set-up)
- [How to Compile](#how-to-compile)
    - [Include Header Files](#include-header-files)
    - [How to compile examples?](#how-to-compile-examples)
    - [How to compile UPC++ benchmarks?](#how-to-compile-upc-benchmarks)
- [How to Run Examples](#how-to-run-examples)
    - [How to Download Data Sets](#how-to-download-data-sets)
- [How to Contribute](#how-to-contribute)
- [How to Configure Environment](#how-to-configure-environment)
- [How to Troubleshoot](#how-to-troubleshoot)
- [Installation and Usage](#installation-and-usage)
- [Working with UPC++](#working-with-upc)
- [Working with Saddlebag](#working-with-saddlebag)
            - [Setting up Saddlebag on Abel](#setting-up-saddlebag-on-abel)
            - [Testing the Installation on Abel](#testing-the-installation-on-abel)
            - [Testing the Installation on Abel with Multiple Nodes](#testing-the-installation-on-abel-with-multiple-nodes)
        - [Installation and Usage on Stallo Cluster at UiT, Norway](#installation-and-usage-on-stallo-cluster-at-uit-norway)
- [Further Reading](#further-reading)

<!-- /MarkdownTOC -->

## Requirements

- [Eigen], a C++ template library
- [CityHash], a C++ library of hash functions for strings

[Eigen]: http://eigen.tuxfamily.org
[CityHash]: https://github.com/google/cityhash

## Source Code for Saddlebags

`src/` directory contains the source code of Saddlebags.
Note that on Abel and possibly other cluster environments, `$(CXX)` in Makefiles may have to be replaced with `mpicxx`.

### Source Code Organisation

Code is organized into the following directories:

- `src` - contains the Saddlebags framework implementation
- `lib/cityhash` - CityHash source code (with license to redistribute this code)
- `examples/pagerank` - regular PageRank implementation
- `examples/tfidf` - tfidf program - datasets for this program is not included

## How to Set Up

Download the source or clone the repository. You can use either of the following repositories:

- <https://github.com/uit-agc/Saddlebag> 

```bash
cd /home/shared/Saddlebag
git clone https://agc-git.cs.uit.no/amin/saddlebags.git
cd saddlebags
make
```

You will also have to set up UPC++ before you can compile Saddlebags.

## How to Compile

```bash
make
```

### Include Header Files

When compiling, following files need to be found in the include path:

- `src/saddlebags.hpp`
- `lib/cityhash/src/city.h`

For this reason when compiling, make sure to include above folders using `-I` flag. See `Makefile` for details.

### How to compile examples?

Create a folder within `examples` folder, and the C++ file of identical name within the new folder. Specify that folder name in `EXAMPLES` variable in `Makefile`.

### How to compile UPC++ benchmarks?

Create a folder within `examples/upcxx` folder, and the C++ file of identical same within the new folder. Specify that folder name below in `BENCHMARKS` variable in `Makefile`.

## How to Run Examples

```bash
export GASNET_PSHM_NODES=4
make
cd examples
./pagerank
./tfidf
```

- Keep in mind that in order to run TF-IDF, you'll first have to download and setup `data/tdidf` folder with Wikipedia dump.
- Keep in mind that in order to run PageRank, you'll first have to download and setup `data/pagerank/simple_graph.txt` file.

### How to Download Data Sets

A small data set file (4.5 MB compressed) is available for initial testing of the examples with Saddlebag, you can download _[dataset-basic.tar.gz]_.

After downloading, extract it to get the `data/` folder.

You need to copy this folder into the root of `saddlebag` directory, making sure it completely overwrites the existing `data` folder already present.

```bash
cd ~/downloads
wget https://.../dataset-basic.tar.gz
tar xzvf dataset-basic.tar.gz
yes | cp -rvf data ~/saddlebags/data/
```

[dataset-basic.tar.gz]: https://trello-attachments.s3.amazonaws.com/59bbad7f5d8c7a986cbea120/5bfc4bcbdf58d60dc629b5d4/1c03934f18467865b7742f3809763e7f/dataset-basic.tar.gz

## How to Contribute

It is recommended to fork the repository, and push your changes upstream when ready.

Alternatively, you can commit your changes into your separate branch, and merge into `master` when done.

The maintainers are developing changes into separate branches, and `master` only contains finished code, which can be compiled and tested.

## How to Configure Environment

You must have UPC++ installed. You must have following environment variables in your bash profile.

```bash
# UPC++
export UPCXX_INSTALL=/home/shared/Saddlebag/install/upcxx-2019.3.0
export GASNET_PHYSMEM_MAX='8000MB'
export GASNET_PSHM_NODES=4
export UPCXX_GASNET_CONDUIT=smp
```

## How to Troubleshoot

- Do not install UPC++ with `sudo` access, as may get errors about old versions of `gcc` and `g++`.
- If you get permission errors about `/home/shared` folder, adjust permissions to allow read and write access to everyone. Do at your own risk, only if you know what are you doing.

```
sudo chmod -R 0777 /home/shared/Saddlebag
```

- When running `tfidf` example, you can see following errors as the program may not be able to read some of the input data files.

```
[Rank 2] ERROR: Unable to open file: ../../data/tfidf/wikidump/abc
```

## Installation and Usage

All compute clusters with InfiniBand or Ethernet connections that support MPI is expected to work, but this text will focus on major Norwegian HPC infrastructure, namely the Abel cluster at University of Oslo, and the Stallo cluster at the University of Tromsø. Usage and installation on clusters differ from single node environments in potential workload managers. The following instructions are expected to be relevant for all clusters with the Slurm workload manager and/or Lmod environment module system. Note that different module naming schemes and workload scheduling policies may apply to different clusters, even if its software is similar.

Installation instructions are available in the UPC++ implementation tarball, and are also included here to provide specific instructions for modules and workload managers found on the aforementioned clusters. An overview of the included Saddlebags applications is provided in a readme file in the source code.

Saddlebags has been implemented and tested with UPC++ v2018.9.0, which can be found at:  
<https://bitbucket.org/upcxx/upcxx/downloads/upcxx-2018.9.0.tar.gz>

All UPC++ versions, and more information about UPC++, is found at:  
<https://bitbucket.org/berkeleylab/upcxx/wiki/Home>

Previously, older versions of UPC++ were available here, but this is no longer available:  
<https://bitbucket.org/upcxx/upcxx/wiki/Home>

## Working with UPC++

Refer to [UPC++ Installation](docs/upcxx.md).

## Working with Saddlebag

##### Setting up Saddlebag on Abel

While the modules are loaded for UPC++, clone the Saddlebag repository. Compile the Saddlebag framework using the installed UPC++ version on the cluster. You can also set the environment variable for convenience.

```bash
cd /desired/saddlebag/directory
make
```

For instance:

```bash
cd ~
git clone git@bitbucket.org:aminmkhan/saddlebag.git
export SADDLEBAG_INSTALL=~/saddlebag
cd $SADDLEBAG_INSTALL
make
$UPCXX_INSTALL/bin/upcxx-run -np 16 ./bin/examples/pagerank
GASNET_PSHM_NODES=4 ./bin/examples/pagerank
```

##### Testing the Installation on Abel

Run the `hello-world` program from UPC++ to make sure UPC++ is properly set up.

```bash
cd $UPCXX_INSTALL/example/prog-guide
$UPCXX_INSTALL/bin/upcxx-run -np 16 ./hello-world
```

Run some of the Saddlebag example programs.

```bash
cd $SADDLEBAG_INSTALL/bin/examples
UPCXX_INSTALL/bin/upcxx-run -np 16 ./pagerank
```

##### Testing the Installation on Abel with Multiple Nodes

In order to run workloads on Abel, you need allocated CPU hours on a specific account name. To find your account name:  

```bash
cost
```

Run programs with the `salloc` command. Memory usage and max compute time must be provided:

```bash
cd $UPCXX_SOURCE/example/prog-guide
make
salloc --nodes=2 --mem-per-cpu=8000 --time=01:00:00 --account=ACCNAME \
$UPCXX_INSTALL/bin/upcxx-run -np 4 ./hello-world
salloc --nodes=16 --mem-per-cpu=8000 --time=01:00:00 --account=$ACCNAME \
$UPCXX_INSTALL/bin/upcxx-run -np 16 ./hello-world
```

Similarly, run one of the example programs from Saddlebag with the `salloc` command.

```bash
cd $SADDLEBAG_INSTALL/bin/examples
salloc --nodes=16 --mem-per-cpu=8000 --time=01:00:00 --account=ACCNAME \
$UPCXX_INSTALL/bin/upcxx-run -np 16 ./pagerank
```

[Lmod]: https://lmod.readthedocs.io

#### Installation and Usage on Stallo Cluster at UiT, Norway

Stallo uses the same module system as Abel, but provides different default modules and names. Here, `mpicxx` can be set as the default C++ compiler during installation:

```bash
module load OpenMPI/2.1.1-GCC-6.4.0-2.28
CC=mpicc CXX=mpicxx ./install /desired/installation/directory
export $UPCXX_INSTALL=/desired/installation/directory
```

Running programs on Stallo is similar as on Abel, but less information is required to queue workloads.  

```bash
cd $UPCXX_SOURCE/example/prog-guide
make
salloc --nodes=16 $UPCXX_INSTALL/bin/upcxx-run -np 16 ./hello-world
```

Information about memory usage per node is not required, but should be added as an argument, like on Abel, if `GASNET_PHYSMEM_MAX` is set above 1 GB.

## Further Reading

You can find more details in the `[docs/](docs/)` folder.

- Programmer's Guide
- Saddlebag API Reference
- Known Issues
- Basic Unix Help
