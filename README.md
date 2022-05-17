# WaterbugDB

Terminal 1:

```bash
$ go build
$ ./waterbugdb --node-id node1 --raft-port 2222 --http-port 8222 --pg-port 6000
```

Terminal 2:

```bash
$ go build
$ ./waterbugdb --node-id node2 --raft-port 2223 --http-port 8223 --pg-port 6001
```

Terminal 3, tell 1 to have 2 follow it:

```bash
$ curl 'localhost:8222/join?followerAddr=localhost:2223&followerId=node2'
```

Terminal 3, now open psql:

```bash
$ psql -h localhost -p 6000
psql -h 127.0.0.1 -p 6000
psql (13.4, server 0.0.0)
Type "help" for help.

phil=> create table x (age int, name text);
CREATE ok
phil=> insert into x values(14, 'garry'), (20, 'ted');
could not interpret result from server: INSERT ok
INSERT ok
phil=> select name, age from x;
  name   | age 
---------+-----
 "garry" |  14
 "ted"   |  20
(2 rows)
```

Now exit `psql` and connect to the other database at port 6001. It
will fail if you try to write to it but SELECTs will work:

```bash
$ psql -h 127.0.0.1 -p 6001
psql (13.4, server 0.0.0)
Type "help" for help.

phil=> select age, name from x;
 age |  name
-----+---------
  20 | "ted"
  14 | "garry"
(2 rows)
```

## References

* [Philip O'Toole on rqlite](https://youtu.be/rqO9PtBkiSQ?t=2332)
* https://github.com/eatonphil/raft-example
* https://yusufs.medium.com/creating-distributed-kv-database-by-implementing-raft-consensus-using-golang-d0884eef2e28
* https://github.com/Jille/raft-grpc-example
* https://github.com/otoolep/hraftd
* https://pkg.go.dev/github.com/hashicorp/raft
* https://github.com/jackc/pgproto3/tree/master/example/pgfortune
