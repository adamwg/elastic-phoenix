Dynamic Phoenix
===============

Background
----------

[Phoenix][] is a shared-memory implementation of the [MapReduce][] parallel
programming framework.  Phoenix was originally developed at Stanford
University.

One limitation of Phoenix is that once a Phoenix MapReduce application has
started running, it is not possible to add more computational power to the job.
This is due to the fact that all the worker threads in a Phoenix job run in a
single process, communicating using in-memory data structures.  Sometimes, as
when using non-dedicated infrastructure, it is not possible to know in advance
how many processors will be available.

Dynamic Scaling
---------------

Dynamic Phoenix allows multiple worker processes to participate in a single
MapReduce job.  These processes can run in separate virtual machines running on
the same physical host, using the [Nahanni][] shared memory mechanism for Linux
KVM.  This allows computational power to be added to a job by launching another
VM, perhaps automatically.  This makes Dynamic Phoenix well-suited to cloud
environments, where resources are allocated as virtual machine instances.

Dynamic scaling is accomplished by splitting computation between a single master
process and at least one worker process.  The master is responsible for
splitting the input data, generating tasks, and merging the final output.  The
worker processes execute the map and reduce tasks.  Communication and
coordination between the master and the workers takes place using a shared
memory region.  When running all processes on the same machine, POSIX shared
memory is used.  When running in separate virtual machines, Nahanni shared
memory is used.

The shared memory region contains the task queue, the input data, the
intermediate values produced by map tasks, and final output values produced by
reduce tasks.

API
---

For the most part, the Dynamic Phoenix API is identical to the original Phoenix
API.  However, there are some key differences:

1. The splitter function takes an additional parameter, a `splitter_mem_ops_t`,
which contains pointers to memory allocation functions.  The splitter must use
these functions to allocate the data returned in the map arguments.  This means
that the common pattern of `mmap`ing a file and returning offsets from the
splitter does not work in Dynamic Phoenix.
2. The splitter might be re-run if it generates too many tasks (see below).
This means that we must be able to reset the splitter's state.  This is done by
passing a negative `req_units` argument to the splitter.
3. The framework provides a new function, `map_reduce_cleanup`, which should be
called by applications when they are done with the results of a MapReduce job.
4. Applications may provide two new functions, `prep` and `cleanup`, which will
be executed only in the master process.  `prep` is called before any other work
is done, and is a good place to open files that may only be present on the node
running the master process.  The `cleanup` function is called by the
`map_reduce_cleanup` function, in the master only.  It is a good place to close
files that were opened in `prep`.
5. `map_reduce_init` now takes two arguments, `argc` and `argv`, which should be
pointers to your application's `argc` and `argv`.  This is because Dynamic
Phoenix applications take some framework arguments, which are stripped and
processed by `map_reduce_init`.  Because of this, it is important to call
`map_reduce_init` before processing any application-specific arguments.

Running an Application
----------------------

Every Dynamic Phoenix application takes the following arguments:

* -h and -v: Are we running on the host (-h) or in a VM (-v)?
* -m and -w: Are we the master (-m) process or a worker (-w)?
* -d <name>: Where to access shared memory.  A POSIX memory object name if -h is
specified, or the path the the Nahanni UIO device if -v is specified.
* -s <size>: The size of the shared memory, in megabytes.
* --: End of Phoenix arguments.  Any arguments after -- are given to the
application.

You must specify at least one of -h or -v, one of -m or -w, -d, and -s on the
commandline.

The master process must be started first.  After that, any number of worker
processes can be started.

Known Issues and Limitations
----------------------------

### glibc Versions

When running processes in multiple virtual machines, it is important that all
machines being used have the same version of glibc installed.  In particular,
because Dynamic Phoenix relies on pthreads structures in inter-VM shared memory,
different pthreads implementations between machines can cause problems.

### Job Size Restrictions
  
The size of the shared memory segment limits the size of job that can be
executed.  All of the split input data must fit in shared memory.  This data is
freed as map tasks run, but some of the intermediate data must fit in shared
memory with it.  The intermediate data is freed as reduce tasks run, but some
final data will be in shared memory with it.  In other words, the amount of
shared memory needed for a given job will depend heavily on the application.

Additionally, the task queue is in shared memory and has a limited number of
task slots.  The framework will try to split data such that the task queue is
not exhausted, but this relies on the application-provided data unit size.  The
framework will increase the number of units in each split until it successfully
splits all the data, allowing for applications that estimate the unit size
(e.g. word count).  Choosing a good estimate will reduce the number of
iterations required to split your data.

We have successfully run the following examples with 4GB of shared memory:

* Histogram with 1.4GB of input.
* Word Count with 10MB of input.
* Linear Regression with 500MB of input.


### Multiple MapReduce Jobs

In standard Phoenix, you can run multiple MapReduce jobs in a single
executable.  For example, a common pattern is to do some computation in one
MapReduce job then sort the output with another.  This is not possible in the
current version of Dynamic Phoneix.

### Number of Workers

Output queues for worker threads are pre-allocated, so the maximum number of
worker threads is defined in `src/distributed.h`.  The default is 32, which at
the default 4 threads per worker allows for 8 workers.

Contact Information
-------------------

Adam Wolfe Gordon <awolfe@cs.ualberta.ca>

Links
-----

[Phoenix]: http://mapreduce.stanford.edu
[MapReduce]: http://labs.google.com/papers/mapreduce.html
[Nahanni]: http://gitorious.com/nahanni
