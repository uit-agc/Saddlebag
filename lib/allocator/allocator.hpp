// This header implements the Allocator concept for the UPC++ shared heap,
// allowing STL and other C++ code to allocate memory in the registered segment.
// Author: Amir Kamil and Dan Bonachea

#ifndef UPCXXC_ALLOCATOR_HPP
#define UPCXXC_ALLOCATOR_HPP

#include <upcxx/upcxx.hpp>

namespace upcxxc { // TBD: what namespace should be used here?

template<typename T>
struct allocator {
  using value_type = T;
  allocator() {}
  template<typename U>
  allocator(const allocator<U> &other) {}
 #if __cplusplus >= 201703
  [[nodiscard]] 
 #endif
  T* allocate(std::size_t n) const {
    if (!upcxx::initialized()) throw std::bad_alloc();
    if (T* result = upcxx::allocate<T>(n).local()) {
      return result;
    }
    throw std::bad_alloc();
  }
  void deallocate(T* ptr, std::size_t n) const {
    if (!upcxx::initialized()) 
      return; // ignore deallocs after the shared heap has disappeared

    upcxx::deallocate(ptr);
  }
};

template<typename T, typename U>
bool operator==(const allocator<T>& lhs, const allocator<U>& rhs) {
  return true;
}

template<typename T, typename U>
bool operator!=(const allocator<T>& lhs, const allocator<U>& rhs) {
  return false;
}

}

#endif
