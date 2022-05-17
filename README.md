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

phil=>
```

## References

* https://github.com/eatonphil/raft-example
* https://yusufs.medium.com/creating-distributed-kv-database-by-implementing-raft-consensus-using-golang-d0884eef2e28
* https://github.com/Jille/raft-grpc-example
* https://github.com/otoolep/hraftd
* https://pkg.go.dev/github.com/hashicorp/raft
* https://github.com/jackc/pgproto3/tree/master/example/pgfortune
