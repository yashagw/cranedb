package plan

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

// Benchmark sizes. numUsers users each get one row; numOrders orders reference a
// user in [1, numUsers]. Override with BENCH_USERS / BENCH_ORDERS env vars to
// crank the join up to headline sizes (e.g. 100000 x 10000).
const (
	defaultBenchUsers  = 10000
	defaultBenchOrders = 1000
)

// setupBenchDB creates a fresh database for benchmarks (mirrors setupTestDB).
func setupBenchDB(b *testing.B) (*transaction.Transaction, *metadata.Manager, func()) {
	tempDir, err := os.MkdirTemp("", "plan_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	dbPath := filepath.Join(tempDir, "benchdb")

	fm, err := file.NewManager(dbPath, 4096)
	if err != nil {
		b.Fatal(err)
	}
	lm, err := log.NewManager(fm, "benchlog")
	if err != nil {
		b.Fatal(err)
	}
	dirtyPageTable := buffer.NewDirtyPageTable()
	bm, err := buffer.NewManager(fm, lm, dirtyPageTable, 200)
	if err != nil {
		b.Fatal(err)
	}
	lockTable := transaction.NewLockTable()
	transactionTable := transaction.NewTransactionTable()
	tmgr := transaction.NewTransactionManager(fm, lm, bm, lockTable, dirtyPageTable, transactionTable)
	tx := tmgr.BeginTransaction()
	md := metadata.NewManager(true, tx)

	cleanup := func() {
		tx.Commit()
		os.RemoveAll(tempDir)
	}
	return tx, md, cleanup
}

func benchSize(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		n := 0
		for _, c := range v {
			if c < '0' || c > '9' {
				return def
			}
			n = n*10 + int(c-'0')
		}
		if n > 0 {
			return n
		}
	}
	return def
}

// drain opens a plan, iterates it fully, and returns the row count.
func drain(b *testing.B, p Plan) int {
	s, err := p.Open()
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	if err := s.BeforeFirst(); err != nil {
		b.Fatal(err)
	}
	count := 0
	for {
		hasNext, err := s.Next()
		if err != nil {
			b.Fatal(err)
		}
		if !hasNext {
			break
		}
		count++
	}
	return count
}

// BenchmarkJoin compares nested-loop (product + select) against hash join on the
// same equi-join: users.id = orders.user_id.
//
// Run head-to-head at the default sizes:
//
//	go test -bench=BenchmarkJoin -benchtime=1x -timeout=30m ./internal/plan/
//
// For a hash-join-only headline number at large sizes:
//
//	BENCH_USERS=100000 BENCH_ORDERS=10000 go test -bench=BenchmarkJoin/HashJoin -benchtime=1x ./internal/plan/
func BenchmarkJoin(b *testing.B) {
	numUsers := benchSize("BENCH_USERS", defaultBenchUsers)
	numOrders := benchSize("BENCH_ORDERS", defaultBenchOrders)

	tx, md, cleanup := setupBenchDB(b)
	defer cleanup()

	// users(id, name)
	usersSchema := record.NewSchema()
	usersSchema.AddIntField("id")
	usersSchema.AddStringField("name", 20)
	if err := md.CreateTable("users", usersSchema, tx); err != nil {
		b.Fatal(err)
	}
	usersLayout, err := md.GetTableLayout("users", tx)
	if err != nil {
		b.Fatal(err)
	}
	usersTS, err := table.NewTableScan(tx, usersLayout, "users")
	if err != nil {
		b.Fatal(err)
	}
	if err := usersTS.BeforeFirst(); err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= numUsers; i++ {
		if err := usersTS.Insert(); err != nil {
			b.Fatal(err)
		}
		if err := usersTS.SetInt("id", i); err != nil {
			b.Fatal(err)
		}
		if err := usersTS.SetString("name", "u"); err != nil {
			b.Fatal(err)
		}
	}
	usersTS.Close()

	// orders(user_id, amount), user_id cycles through [1, numUsers]
	ordersSchema := record.NewSchema()
	ordersSchema.AddIntField("user_id")
	ordersSchema.AddIntField("amount")
	if err := md.CreateTable("orders", ordersSchema, tx); err != nil {
		b.Fatal(err)
	}
	ordersLayout, err := md.GetTableLayout("orders", tx)
	if err != nil {
		b.Fatal(err)
	}
	ordersTS, err := table.NewTableScan(tx, ordersLayout, "orders")
	if err != nil {
		b.Fatal(err)
	}
	if err := ordersTS.BeforeFirst(); err != nil {
		b.Fatal(err)
	}
	for i := range numOrders {
		if err := ordersTS.Insert(); err != nil {
			b.Fatal(err)
		}
		if err := ordersTS.SetInt("user_id", (i%numUsers)+1); err != nil {
			b.Fatal(err)
		}
		if err := ordersTS.SetInt("amount", i); err != nil {
			b.Fatal(err)
		}
	}
	ordersTS.Close()

	newUsersPlan := func() Plan {
		p, err := NewTablePlan("users", tx, md)
		if err != nil {
			b.Fatal(err)
		}
		return p
	}
	newOrdersPlan := func() Plan {
		p, err := NewTablePlan("orders", tx, md)
		if err != nil {
			b.Fatal(err)
		}
		return p
	}

	// id = user_id
	joinPred := query.NewPredicate(*query.NewTerm(
		*query.NewFieldNameExpression("id"),
		*query.NewFieldNameExpression("user_id"),
		query.OpEQ,
	))

	b.Run("NestedLoop", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			var plan Plan = NewSelectPlan(NewProductPlan(newUsersPlan(), newOrdersPlan()), joinPred)
			got := drain(b, plan)
			if got != numOrders {
				b.Fatalf("nested loop produced %d rows, want %d", got, numOrders)
			}
		}
	})

	b.Run("HashJoin", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			var plan Plan = NewHashJoinPlan(newUsersPlan(), newOrdersPlan(), "id", "user_id")
			got := drain(b, plan)
			if got != numOrders {
				b.Fatalf("hash join produced %d rows, want %d", got, numOrders)
			}
		}
	})
}
