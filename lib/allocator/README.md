# UPC++ STL Allocator #

This header implements the Allocator concept for the UPC++ shared heap,
allowing STL and other C++ code to allocate memory in the registered segment.

This allows you to declare STL objects with types like:

```
std::vector<double,upcxxc::allocator<double>> my_vector;
```

and have the backing storage reside in the UPC++ shared heap.

For a more extensive example, see [example.cpp](example.cpp).

Note that because the storage for objects using this allocator reside
in the UPC++ shared heap, they may only be created and used in the 
interval between the `upcxx::init()` and `upcxx::finalize()` calls
that initialize and de-initialize the library, and within the same such
interval, for any given object.
This further implies that such objects may not have static storage
duration, since their creation would precede UPC++ library initialization.


