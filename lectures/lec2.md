# RPC And Threads

## Why Golang For This Course

- Memory-safe, _i.e_ cannot accidentally access uninitialized memory, free memory
  memory more than once, ... _etc_
- Good concurrency support, with constructs for locking, synchronization, ... _etc_
- Garbage-collected
- Much simpler than a language like C++

## Why threads in a distributed system?

- Concurrency for IO-bound tasks
- parallelism for CPU-bound tasks
- Background tasks such as checking if workers are alive

This threading approach to concurrency can be constrasted with an event-driven
approach: a single thread of execution that keeps state about all the background
tasks it's firing off.

## Student Question: Difference Between a Thread and a Process?

The answer is for UNIX systems: a process is a single program you're running, which has
its own address space and own memory area. A process may have several threads running
within it. So for example, running a Golang program creates a single process with its
own process id (PID), and then the process may have multiple go-routines or threads
that all can share memory, synchronize channels, use mutexes ... _etc_. However, at
the process level, the OS keeps each process in its own sandbox, and they cannot
share resources, and there generally isn't much resource sharing.

## Threading Challenges

- Cool thing: All threads in a process exist in the same memory space. If a thread
  creates an object in memory, it can let other threads use it. However, without
  locking this can lead to race conditions and bugs.
- Coordination: Most of the time each thread is executing without regard to other
  threads, but sometimes we may want to synchronize different threads and have them
  interact, _e.g._ wait on the result of another thread. Golang offers several
  constructs for coordinates:
  - channels
  - wait groups (launching a known number of goroutines and waiting for them to finish)
  - condition variables
- Deadlocks: A scenario in which threads cannot proceed due to conflict in locks.

## Student Question: How does a Mutex know about which variables it's locking?

## Student Question: Is it better to keep a lock as a global variable, or a private member of a data structure?

## Student Question on Golang passing by value vs. passing by reference

In Golang, the map is a reference type, so when you pass it to a function it's automatically
passed by reference, basically a map itself is a pointer. Structs however are passed by value,
so if you want the function to modify them, you have to explicitly pass a pointer to them via
the address-of operator.

## Student Question: What happens if a go-routine fails without the program itself failing?

## Student Question: Does the anonymously declared Goroutine have to take the url as a parameter? Why can it not use it from the closure?

## Student Question: If an inner function (closure) is referring to a variable that is defined and allocated in the outer function, what happens to that reference if the outer function returns?

## Question: How many go-routines does this program launch?
