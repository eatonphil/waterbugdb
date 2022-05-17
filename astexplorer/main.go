package main

import (
	"fmt"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v2"
)

func main() {
	r, err := pgquery.Parse("CREATE TABLE x (name TEXT, age INT); INSERT INTO x VALUES ('what', 12); SELECT name, id FROM x")
	if err != nil {
		panic(err)
	}

	for i, stmt := range r.GetStmts() {
		if i > 0 {
			fmt.Println("\n")
		}

		n := stmt.GetStmt()
		if c := n.GetCreateStmt(); c != nil {
			fmt.Println("CREATE", c.Relation.Relname)
			for _, c := range c.TableElts {
				cd := c.GetColumnDef()
				fmt.Println("CREATE FIELD", cd.Colname)
				// Names is namespaced. So `INT` is pg_catalog.int4. `BIGINT` is pg_catalog.int8.
				var fullName []string
				for _, n := range cd.TypeName.Names {
					fullName = append(fullName, n.GetString_().Str)
				}
				fmt.Println("CREATE FIELD", strings.Join(fullName, "."))
			}

			continue
		}

		if i := n.GetInsertStmt(); i != nil {
			fmt.Println("INSERT", i.Relation.Relname)
			slct := i.GetSelectStmt().GetSelectStmt()
			for _, values := range slct.ValuesLists {
				for _, value := range values.GetList().Items {
					if c := value.GetAConst(); c != nil {
						if s := c.Val.GetString_(); s != nil {
							fmt.Println("A STRING", s.Str)
							continue
						}

						if i := c.Val.GetInteger(); i != nil {
							fmt.Println("AN INT", i.Ival)
							continue
						}

						fmt.Println("Unknown value type", value)
						continue
					}

					fmt.Println("unknown value type", value)
				}
			}

			continue
		}

		if s := n.GetSelectStmt(); s != nil {
			for _, c := range s.TargetList {
				fmt.Println("SELECT", c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().Str)
			}
			fmt.Println("SELECT FROM", s.FromClause[0].GetRangeVar().Relname)
			continue
		}
		
		fmt.Println("UNKNOWN type: %s", stmt)
	}
}
