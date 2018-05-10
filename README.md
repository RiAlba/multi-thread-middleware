# Multi-thread Middleware

A multi-threaded Java middleware platform for a memcached key-value store with memtier workloads.

## What's it about?

This project consists of a middleware platform for key-value stores developed in Java. Specifically, it works with _memcached_, which is a very commonly used main-memory key-value store. The clients of the middleware must be workloads generated using _memtier_.

In this project we will only rely on two memcached operations: ``GETs`` and ``SETs``.
* ``GET`` operations are used to read a value associated with one or more keys. 
* ``SET`` operations are used to insert or update the value belonging to a key.

## References 

1. https://memcached.org/

2. https://github.com/RedisLabs/memtier_benchmark/
