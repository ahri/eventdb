# EventDB

A simple database to store a stream of events (facts) and retrieve them by
index. This is aimed at systems based on
[Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) where
complex state modelling/querying is left to other subsystems (e.g. your app, or
a relational/graph database).

The database is thread-safe for concurrent reads/writes, though writes are
locked to operate sequentially.

## Platforms
Only POSIX (i.e. non-Windows) platforms are supported.


## [ACID](https://en.wikipedia.org/wiki/ACID)
A database is expected to make guarantees about the data it holds, which are
collectively known as "ACID". Note in particular that all guarantees hold only
on systems/filesystems that honour the contract of
[`writev`](http://man7.org/linux/man-pages/man2/writev.2.html), i.e. that "The
data transfers performed by readv() and writev() are atomic: the data written by
writev() is written as a single block that is not intermingled with output from
writes in other processes", and also honour the `O_SYNC` option on file
descriptors as specified in
[`open`](http://man7.org/linux/man-pages/man2/open.2.html) which behaves as if
[`fsync`](http://man7.org/linux/man-pages/man2/fdatasync.2.html) was called
after each write, forcing data through the userspace and kernel to ensure it's
flushed to the physical disk drive. Failures of a physical device to atomically
write its buffers are not accounted for.

Isolation is catered for via [STM](https://hackage.haskell.org/package/stm);
writes are queued after an STM commit. Setting a queue size to `1` will result
in immediate writes in cases where performance matters less than saving data
quickly.

### Atomicity
Grouped writes (i.e. transactions) to the database are guaranteed to be atomic,
i.e. they will either all be written, or none of them will be.

### Consistency
The database is guaranteed to remain consistent after each successful or failing
write. This is trivial to achieve with such a simple database where there is no
support for multiple tables and the relationships between them.

### Isolation
Client code run in [STM](https://hackage.haskell.org/package/stm) gets the
benefits of isolation through maintaining its own state and invalidating
transactions at a more fine-grained level than can be achieved at the database
level. Put another way; the database could invalidate any transactions if a
change has taken place in the meantime - but this is like using a sledgehammer
to crack a nut. On the other hand, client code knows in great detail whether or
not a transaction is valid, so passing off the isolation responsibility allows
for more efficient operation. See [demo](app-bank-acct-demo/Main.hs) as an
example - examine in particular the `transact`, `exec` and `apply` functions.

### Durability
Successful writes to the database are guaranteed to persist even in the case of
system failures like a crash or power outage. Uncommitted/interrupted writes
will not affect the state of the database once the system is operational.

## Testing
A [sanity](test/sanity.sh) test script is run as a pre-commit hook to ensure
that the database maintains consistency.

The excellent [libfiu](https://blitiri.com.ar/p/libfiu/) is used to introduce
write errors, allowing tests on consistency - see
[script](test/eventdb-acd-test.sh). In addition power-pull tests were executed
on a laptop with a consumer-grade SSD.

## TODO
- .cabal file with tested lower bounds
- Streaming test:
  - write 512MB of data
  - call `readEvents 0`
  - write another 512MB of data
  - quit once all data has been consumed (Sum it)
  - verify max memory usage
  - improve `readEvents` api to single TChan if possible
- Add execution of demo apps to sanity check
- Haddocks
- Property-based tests
  - Randomly generated transactions, following close and read,
    `output == join input`
