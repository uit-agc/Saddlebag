#include <upcxx/upcxx.hpp>
#include "allocator.hpp"

#include <vector>
#include <cassert>
#include <iostream>

struct particle_t {
  float x, y, z, vx, vy, vz, charge;
  uint64_t id;
};

typedef std::vector<particle_t,upcxxc::allocator<particle_t>> uvector;

int main() {

  upcxx::init();

    // ---------------------------------------------------------------
    // use case 1: a vector containing a basic type

    // create a vector in shared space
    std::vector<double,upcxxc::allocator<double>> my_vector;

    double val = 3.14;
    my_vector.push_back(val); // populate

    // create a global pointer to the elements
    upcxx::global_ptr<double> gdp = upcxx::try_global_ptr(my_vector.data());
    assert(gdp != nullptr);

    // verify the contents
    double res = upcxx::rget(gdp).wait();
    assert(res == val);

    upcxx::barrier();
    // ---------------------------------------------------------------
    // use case 2: a vector containing a trivially copyable user class

    // create a particle containing my id
    particle_t mypart;
    mypart.id = upcxx::rank_me();

    uvector myvec; // create a vector in shared space

    upcxx::global_ptr<particle_t> lz;
    if (!upcxx::rank_me()) {
      myvec.reserve(upcxx::rank_n()); // reserve a landing zone for each peer
      // create a global pointer to the elements
      lz = upcxx::try_global_ptr(myvec.data());
      assert(lz != nullptr);
    }

    // propagate element pointer to each rank
    lz = upcxx::broadcast(lz, 0).wait(); 
    lz += upcxx::rank_me();

    // write my particle to my element in the vector on rank 0
    upcxx::rput(mypart, lz).wait();

    upcxx::barrier(); // await all contributions

    if (!upcxx::rank_me()) { // validate
      for (upcxx::intrank_t r = 0; r < upcxx::rank_n(); r++) {
        uint64_t id = myvec[r].id;
        assert(id == r);
      }
    }
    upcxx::barrier();
    // ---------------------------------------------------------------
    
    if (!upcxx::rank_me()) std::cout << "SUCCESS" << std::endl;

  upcxx::finalize();

  return 0;
}

