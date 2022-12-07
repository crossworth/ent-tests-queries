package bug

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"

	"entgo.io/bug/ent/node"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"entgo.io/bug/ent"
	"entgo.io/bug/ent/enttest"
)

func TestBugSQLite(t *testing.T) {
	client := enttest.Open(t, dialect.SQLite, "file:ent?mode=memory&cache=shared&_fk=1")
	defer client.Close()
	test(t, client)
}

func TestBugMySQL(t *testing.T) {
	for version, port := range map[string]int{"56": 3306, "57": 3307, "8": 3308} {
		addr := net.JoinHostPort("localhost", strconv.Itoa(port))
		t.Run(version, func(t *testing.T) {
			client := enttest.Open(t, dialect.MySQL, fmt.Sprintf("root:pass@tcp(%s)/test?parseTime=True", addr))
			defer client.Close()
			test(t, client)
		})
	}
}

func TestBugPostgres(t *testing.T) {
	for version, port := range map[string]int{"10": 5430, "11": 5431, "12": 5432, "13": 5433, "14": 5434} {
		t.Run(version, func(t *testing.T) {
			client := enttest.Open(t, dialect.Postgres, fmt.Sprintf("host=localhost port=%d user=postgres dbname=test password=pass sslmode=disable", port))
			defer client.Close()
			test(t, client)
		})
	}
}

func TestBugMaria(t *testing.T) {
	for version, port := range map[string]int{"10.5": 4306, "10.2": 4307, "10.3": 4308} {
		t.Run(version, func(t *testing.T) {
			addr := net.JoinHostPort("localhost", strconv.Itoa(port))
			client := enttest.Open(t, dialect.MySQL, fmt.Sprintf("root:pass@tcp(%s)/test?parseTime=True", addr))
			defer client.Close()
			test(t, client)
		})
	}
}

func test(t *testing.T, client *ent.Client) {
	ctx := context.Background()
	client.Node.Delete().ExecX(ctx)
	client.Node.Create().SetFrom("Events").SetTo("Users").SaveX(ctx)
	client.Node.Create().SetFrom("Events").SetTo("Orders").SaveX(ctx)
	client.Node.Create().SetFrom("Orders").SetTo("Users").SaveX(ctx)

	var nodes []struct {
		Node     string `json:"name"`
		InCount  int    `json:"incount"`
		OutCount int    `json:"outcount"`
	}
	client.Node.Query().Modify(func(s *sql.Selector) {
		// union both types
		tb := sql.Table(node.Table)
		n1 := sql.Select(sql.As(tb.C(node.FieldFrom), "name")).From(tb)
		n2 := sql.Select(sql.As(tb.C(node.FieldTo), "name")).From(tb)
		q := n1.Union(n2).As("q")

		// create a join
		in := sql.Table(node.Table).As("i")
		out := sql.Table(node.Table).As("o")
		s.From(q).LeftJoin(in).On(q.C("name"), in.C(node.FieldFrom))
		s.LeftJoin(out).On(q.C("name"), out.C(node.FieldTo))

		s.Select(
			sql.Distinct(q.C("name")),
			sql.As(sql.Count(in.C(node.FieldFrom)), "incount"),
			sql.As(sql.Count(out.C(node.FieldTo)), "outcount"),
		)
		s.GroupBy(q.C("name"))
	}).ScanX(ctx, &nodes)
	for _, n := range nodes {
		fmt.Println(n.Node, n.InCount, n.OutCount)
	}

	// Events 2 0
	// Orders 1 1
	// Users 0 2
	require.Len(t, nodes, 3)
	require.Equal(t, "Events", nodes[0].Node)
	require.Equal(t, 2, nodes[0].InCount)
	require.Equal(t, 0, nodes[0].OutCount)
	require.Equal(t, "Orders", nodes[1].Node)
	require.Equal(t, 1, nodes[1].InCount)
	require.Equal(t, 1, nodes[1].OutCount)
	require.Equal(t, "Users", nodes[2].Node)
	require.Equal(t, 0, nodes[2].InCount)
	require.Equal(t, 2, nodes[2].OutCount)
}
