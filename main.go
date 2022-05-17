package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	bolt "go.etcd.io/bbolt"
)

type pgEngine struct {
	db *bolt.DB
}

type pgResult struct {
	fieldNames []string
	fieldTypes []string
	rows       [][]any
}

type tableDefinition struct {
	Name        string
	ColumnNames []string
	ColumnTypes []string
}

func (pe *pgEngine) executeCreate(stmt *pgquery.CreateStmt) (*pgResult, error) {
	tbl := tableDefinition{}
	tbl.Name = stmt.Relation.Relname

	for _, c := range stmt.TableElts {
		cd := c.GetColumnDef()

		tbl.ColumnNames = append(tbl.ColumnNames, cd.Colname)

		// Names is namespaced. So `INT` is pg_catalog.int4. `BIGINT` is pg_catalog.int8.
		var columnType string
		for _, n := range cd.TypeName.Names {
			if columnType != "" {
				columnType += "."
			}
			columnType += n.GetString_().Str
		}
		tbl.ColumnTypes = append(tbl.ColumnTypes, columnType)
	}

	tableBytes, err := json.Marshal(tbl)
	if err != nil {
		return nil, fmt.Errorf("Could not marshal table: %s", err)
	}

	err = pe.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("data")).Put([]byte("tables_"+tbl.Name), tableBytes)
	})

	if err != nil {
		return nil, fmt.Errorf("Could not set key-value: %s", err)
	}

	return nil, nil
}

func (pe *pgEngine) getTableDefinition(name string) (*tableDefinition, error) {
	var tbl tableDefinition

	err := pe.db.View(func(tx *bolt.Tx) error {
		valBytes := tx.Bucket([]byte("data")).Get([]byte("tables_" + name))
		err := json.Unmarshal(valBytes, &tbl)
		if err != nil {
			return fmt.Errorf("Could not unmarshal table: %s", err)
		}

		return nil
	})

	return &tbl, err
}

func (pe *pgEngine) executeInsert(stmt *pgquery.InsertStmt) (*pgResult, error) {
	tblName := stmt.Relation.Relname

	slct := stmt.GetSelectStmt().GetSelectStmt()
	for _, values := range slct.ValuesLists {
		var rowData []any
		for _, value := range values.GetList().Items {
			if c := value.GetAConst(); c != nil {
				if s := c.Val.GetString_(); s != nil {
					rowData = append(rowData, s.Str)
					continue
				}

				if i := c.Val.GetInteger(); i != nil {
					rowData = append(rowData, i.Ival)
					continue
				}
			}

			return nil, fmt.Errorf("Unknown value type: %s", value)
		}

		rowBytes, err := json.Marshal(rowData)
		if err != nil {
			return nil, fmt.Errorf("Could not marshal row: %s", err)
		}

		id := uuid.New().String()
		err = pe.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("data")).Put([]byte("rows_"+tblName+"_"+id), rowBytes)
		})
		if err != nil {
			return nil, fmt.Errorf("Could not store row: %s", err)
		}
	}

	return nil, nil
}

func (pe *pgEngine) executeSelect(stmt *pgquery.SelectStmt) (*pgResult, error) {
	tblName := stmt.FromClause[0].GetRangeVar().Relname
	tbl, err := pe.getTableDefinition(tblName)
	if err != nil {
		return nil, err
	}

	var results *pgResult
	for _, c := range stmt.TargetList {
		fieldName := c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().Str
		results.fieldNames = append(results.fieldNames, fieldName)

		fieldType := ""
		for i, cn := range tbl.ColumnNames {
			if cn == fieldName {
				fieldType = tbl.ColumnTypes[i]
			}
		}

		if fieldType == "" {
			return nil, fmt.Errorf("Unknown field: %s", fieldName)
		}

		results.fieldTypes = append(results.fieldTypes, fieldType)
	}

	prefix := []byte("rows_" + tblName + "_")
	pe.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("data")).Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var row []any
			err = json.Unmarshal(v, &row)
			if err != nil {
				return fmt.Errorf("Unable to unmarshal row: %s", err)
			}

			var targetRow []any
			for i, field := range tbl.ColumnNames {
				for _, target := range results.fieldNames {
					if target == field {
						targetRow = append(targetRow, row[i])
					}
				}
			}

			results.rows = append(results.rows, targetRow)
		}

		return nil
	})

	return results, nil
}

func (pe *pgEngine) executeStatement(n *pgquery.Node) (*pgResult, error) {
	if c := n.GetCreateStmt(); c != nil {
		return pe.executeCreate(c)
	}

	if c := n.GetInsertStmt(); c != nil {
		return pe.executeInsert(c)
	}

	if c := n.GetSelectStmt(); c != nil {
		return pe.executeSelect(c)
	}

	return nil, fmt.Errorf("Unknown statement type: %s", n)
}

func (pe *pgEngine) execute(tree *pgquery.ParseResult) (*pgResult, error) {
	var res *pgResult
	var err error
	for _, stmt := range tree.GetStmts() {
		res, err = pe.executeStatement(stmt.GetStmt())
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

type pgFsm struct {
	pe *pgEngine
}

func (pf *pgFsm) Apply(log *raft.Log) any {
	switch log.Type {
	case raft.LogCommand:
		ast, err := pgquery.Parse(string(log.Data))
		if err != nil {
			return fmt.Errorf("Could not parse payload: %s", err)
		}

		_, err = pf.pe.execute(ast)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unknown raft log type: %#v", log.Type)
	}

	return nil
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error { return nil }
func (sn snapshotNoop) Release()                          {}

func (pf *pgFsm) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

func (pf *pgFsm) Restore(rc io.ReadCloser) error {
	// deleting first isn't really necessary since there's no exposed DELETE operation anyway.
	// so any changes over time will just get naturally overwritten

	decoder := json.NewDecoder(rc)

	for decoder.More() {
		var sp string
		err := decoder.Decode(&sp)
		if err != nil {
			return fmt.Errorf("Could not decode payload: %s", err)
		}

		ast, err := pgquery.Parse(sp)
		if err != nil {
			return fmt.Errorf("Could not parse payload: %s", err)
		}

		_, err = pf.pe.execute(ast)
		if err != nil {
			return err
		}
	}

	return rc.Close()
}

func setupRaft(dir, nodeId, raftAddress string, pf *pgFsm) (*raft.Raft, error) {
	os.MkdirAll(dir, os.ModePerm)

	store, err := raftboltdb.NewBoltStore(path.Join(dir, "bolt"))
	if err != nil {
		return nil, fmt.Errorf("Could not create bolt store: %s", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(path.Join(dir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("Could not create snapshot store: %s", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, fmt.Errorf("Could not resolve address: %s", err)
	}

	transport, err := raft.NewTCPTransport(raftAddress, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("Could not create tcp transport: %s", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(nodeId)

	r, err := raft.NewRaft(raftCfg, pf, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("Could not create raft instance: %s", err)
	}

	// Cluster consists of unjoined leaders. Picking a leader and
	// creating a real cluster is done manually after startup.
	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(nodeId),
				Address: transport.LocalAddr(),
			},
		},
	})

	return r, nil
}

type httpServer struct {
	r *raft.Raft
}

func (hs httpServer) joinHandler(w http.ResponseWriter, r *http.Request) {
	followerId := r.URL.Query().Get("followerId")
	followerAddr := r.URL.Query().Get("followerAddr")

	if hs.r.State() != raft.Leader {
		json.NewEncoder(w).Encode(struct {
			Error string `json:"error"`
		}{
			"Not the leader",
		})
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	err := hs.r.AddVoter(raft.ServerID(followerId), raft.ServerAddress(followerAddr), 0, 0).Error()
	if err != nil {
		log.Printf("Failed to add follower: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}

	w.WriteHeader(http.StatusOK)
}

var dataTypeOIDMap = map[string]uint32{
	"text":            25,
	"pg_catalog.int4": 23,
}

func writePgResult(res *pgResult, conn net.Conn) {
	rd := &pgproto3.RowDescription{}
	for i, field := range res.fieldNames {
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:        []byte(field),
			DataTypeOID: dataTypeOIDMap[res.fieldTypes[i]],
		})
	}
	buf := rd.Encode(nil)
	for _, row := range res.rows {
		dr := &pgproto3.DataRow{}
		for _, value := range row {
			bs, err := json.Marshal(value)
			if err != nil {
				log.Printf("Failed to marshal cell: %s\n", err)
				return
			}

			dr.Values = append(dr.Values, bs)
		}

		buf = dr.Encode(buf)
	}

	buf = (&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(res.rows)))}).Encode(buf)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	_, err := conn.Write(buf)
	if err != nil {
		log.Printf("Failed to write query response: %s", err)
	}
}

func runPgServer(port string, db *bolt.DB, r *raft.Raft) {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go func() {
			pgconn := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
			defer conn.Close()

			for {
				startupMessage, err := pgconn.ReceiveStartupMessage()
				if err != nil {
					log.Printf("error receiving startup message: %s\n", err)
					return
				}

				switch startupMessage.(type) {
				case *pgproto3.StartupMessage:
					buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
					buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
					_, err = conn.Write(buf)
					if err != nil {
						log.Printf("Error sending ready for query: %s\n", err)
						return
					}
					break
				case *pgproto3.SSLRequest:
					_, err = conn.Write([]byte("N"))
					if err != nil {
						log.Printf("Error sending deny SSL request: %s\n", err)
						return
					}
				default:
					log.Printf("Unknown startup message: %#v\n", startupMessage)
					return
				}
			}

			for {
				msg, err := pgconn.Receive()
				if err != nil {
					log.Printf("Error receiving message: %s\n", err)
					return
				}

				switch t := msg.(type) {
				case *pgproto3.Query:
					stmts, err := pgquery.Parse(t.String)
					if err != nil {
						log.Printf("Error parsing query: %s\n", err)
						return
					}

					if len(stmts.GetStmts()) > 1 {
						log.Println("Only make one request at a time.")
						return
					}

					stmt := stmts.GetStmts()[0]

					// Handle SELECTs here
					s := stmt.GetStmt().GetSelectStmt()
					if s != nil {
						pe := &pgEngine{db}
						res, err := pe.executeSelect(s)
						if err != nil {
							log.Println(err)
							return
						}

						writePgResult(res, conn)
						continue
					}

					// Otherwise it's DDL/DML, raftify
					future := r.Apply([]byte(t.String), 500*time.Millisecond)
					if err := future.Error(); err != nil {
						log.Printf("Could not apply: %s", err)
						return
					}

					e := future.Response()
					if e != nil {
						log.Printf("Could not apply (internal): %s", e)
						return
					}
				case *pgproto3.Terminate:
					return
				default:
					log.Printf("Received message other than Query from client: %s\n", msg)
					return
				}
			}
		}()
	}
}

type config struct {
	id       string
	httpPort string
	raftPort string
	pgPort   string
}

func getConfig() config {
	cfg := config{}
	for i, arg := range os.Args[1:] {
		if arg == "--node-id" {
			cfg.id = os.Args[i+2]
			i++
			continue
		}

		if arg == "--http-port" {
			cfg.httpPort = os.Args[i+2]
			i++
			continue
		}

		if arg == "--raft-port" {
			cfg.raftPort = os.Args[i+2]
			i++
			continue
		}

		if arg == "--pg-port" {
			cfg.pgPort = os.Args[i+2]
			i++
			continue
		}
	}

	if cfg.id == "" {
		log.Fatal("Missing required parameter: --node-id")
	}

	if cfg.raftPort == "" {
		log.Fatal("Missing required parameter: --raft-port")
	}

	if cfg.httpPort == "" {
		log.Fatal("Missing required parameter: --http-port")
	}

	if cfg.pgPort == "" {
		log.Fatal("Missing required parameter: --pg-port")
	}

	return cfg
}

func main() {
	cfg := getConfig()

	dataDir := "data"
	err := os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		log.Fatalf("Could not create data directory: %s", err)
	}

	db, err := bolt.Open(path.Join(dataDir, "/data"+cfg.id), 0600, nil)
	if err != nil {
		log.Fatalf("Could not open bolt db: %s", err)
	}
	defer db.Close()

	pe := &pgEngine{db}
	pf := &pgFsm{pe}

	r, err := setupRaft(path.Join(dataDir, "raft"+cfg.id), cfg.id, "localhost:"+cfg.raftPort, pf)
	if err != nil {
		log.Fatal(err)
	}

	hs := httpServer{r}
	http.HandleFunc("/join", hs.joinHandler)
	go func() {
		err := http.ListenAndServe(":"+cfg.httpPort, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	runPgServer(cfg.pgPort, db, r)
}
