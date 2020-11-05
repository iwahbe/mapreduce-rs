# Map Reduce

In 2004, engineers at Google introduced a new paradigm for large-scale
parallel data processing known as MapReduce (see the original paper
[here](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf),
and make sure to look in the citations at the end). One key aspect of
MapReduce is that it makes programming such tasks on large-scale clusters easy
for developers; instead of worrying about how to manage parallelism, handle
machine crashes, and many other complexities common within clusters of
machines, the developer can instead just focus on writing little bits of code
(described below) and the infrastructure handles the rest.

## Installation Instructions

This map reduce project was written in Rust. To install, build and test it,
simply clone this repository onto your machine and run the command `make test`.
This will run the tests written in the make file, as well as the tests internal
to the rust library we have written. If Rust is not installed, `make` will prompt
you to install it.

## Compiling

The project assumes that both `clang` and either `cargo` (the rust build tool)
or `curl` (to install rust from) is in your path. If cargo is not found in
`PATH`, rust should be automatically installed. If clang is not installed, best
of luck. `make` is used to build the C interface, as well as run integration
tests. Unit tests are run from within `cargo`.

## Overview

The library is linked against the C header below.
[`mapreduce.h`](https://github.com/remzi-arpacidusseau/ostep-projects/tree/master/concurrency-mapreduce/mapreduce.h)
header file that specifies exactly what you must build in your MapReduce library:

```
#ifndef __mapreduce_h__
#define __mapreduce_h__

// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// External functions: these are what you must define
void MR_Emit(char *key, char *value);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[],
	    Mapper map, int num_mappers,
	    Reducer reduce, int num_reducers,
	    Partitioner partition);

#endif // __mapreduce_h__
```

The most important function is `MR_Run`, which takes the command line
parameters of a given program, a pointer to a Map function (type `Mapper`,
called `map`), the number of mapper threads your library should create
(`num_mappers`), a pointer to a Reduce function (type `Reducer`, called
`reduce`), the number of reducers (`num_reducers`), and finally, a pointer to
a Partition function (`partition`, described below).

Thus, when a user is writing a MapReduce computation, they implement a Map
function, a Reduce function, possibly a Partition function, and then call
`MR_Run()`. The infrastructure will create threads as appropriate and run the
computation.

One basic assumption is that the library will create `num_mappers` threads
(in a thread pool) that perform the map tasks. Another is that your library
will create `num_reducers` threads to perform the reduction tasks.

## Simple Example: Wordcount

Here is a simple (but functional) `wordcount` program, written to use this
infrastructure:

```
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
    int count = 0;
    char *value;
    while ((value = get_next(key, partition_number)) != NULL)
        count++;
    printf("%s %d\n", key, count);
}

int main(int argc, char *argv[]) {
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
}
```

Let's walk through this code, in order to see what it is doing. First, notice
that `Map()` is called with a file name. In general, we assume that this type
of computation is being run over many files; each invocation of `Map()` is
thus handed one file name and is expected to process that file in its
entirety.

In this example, the code above just reads through the file, one line at a
time, and uses `strsep()` to chop the line into tokens. Each token is then
emitted using the `MR_Emit()` function, which takes two strings as input: a
key and a value. The key here is the word itself, and the token is just a
count, in this case, 1 (as a string). It then closes the file.

The `MR_Emit()` function is thus another key part of your library; it needs to
take key/value pairs from the many different mappers and store them in a way
that later reducers can access them, given constraints described
below. Designing and implementing this data structure is thus a central
challenge of the project.

After the mappers are finished, your library should have stored the key/value
pairs in such a way that the `Reduce()` function can be called. `Reduce()` is
invoked once per key, and is passed the key along with a function that enables
iteration over all of the values that produced that same key. To iterate, the
code just calls `get_next()` repeatedly until a NULL value is returned;
`get_next` returns a pointer to the value passed in by the `MR_Emit()`
function above, or NULL when the key's values have been processed. The output,
in the example, is just a count of how many times a given word has appeared,
and is just printed to standard output.

All of this computation is started off by a call to `MR_Run()` in the `main()`
routine of the user program. This function is passed the `argv` array, and
assumes that `argv[1]` ... `argv[n-1]` (with `argc` equal to `n`) all contain
file names that will be passed to the mappers.

One interesting function that you also need to pass to `MR_Run()` is the
partitioning function. In most cases, programs will use the default function
(`MR_DefaultHashPartition`), which should be implemented by your code. Here is
its implementation:

```
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
```

The function's role is to take a given `key` and map it to a number, from `0`
to `num_partitions - 1`. Its use is internal to the MapReduce library, but
critical. Specifically, your MR library should use this function to decide
which partition (and hence, which reducer thread) gets a particular key/list
of values to process. For some applications, which reducer thread processes a
particular key is not important (and thus the default function above should be
passed in to `MR_Run()`); for others, it is, and this is why the user can pass
in their own partitioning function as need be.

One last requirement: For each partition, keys (and the value list associated
with said keys) should be **sorted** in ascending key order; thus, when a
particular reducer thread (and its associated partition) are working, the
`Reduce()` function should be called on each key in order for that partition.

## Code

The project is broken up into 3 files, `ccompat.rs`, `threadpool.rs` and
`lib.rs`.

The first provides a non-owning wrapper around an array, as well as a
convenience wrapper to get around rust's dislike of passing `*const T` between
threads.

The second provides a `threadPool` for use by `mappers` and a `ReducePool` for
reducers. A `ThreadPool` is exactly what you expect it to be and a `ReuducePool`
is similar. Instead of executing a set of operations in the next available
thread, a `ReducePool` executes a set of operations in the same thread, each
additional set of operations spawning a new thread on which to execute.

Finally `lib.rs` provides the interface as well as the main data structure:
`GlobalEmitter`. Because of the C header given, we needed to use global mutable
state. This involved two main abstractions. A `StableMutex` allows us to create
a threadsafe object that is fully mutable, then permanently lock it, and forgo
the cost of the `Mutex`. A `DefaultDashMap` is a concurrent hash map built on
top of the `DashMap` library, that supports the creation of complex data
structures as map keys. In this case, we use `BinaryHeap`s, but we could in
theory use any concurrent data structure. The `EMITTED`, once stabilized,
acts as a statically allocated wrapper for a `DefualtDashMap`. The functions
exposed to C, expecting `MR_Run`, are simple interfaces into `EMITTED`,

## Citations

This project was complies with the ostep operating systems textbook and was
written for Reed's CS393 Operating Systems class.
