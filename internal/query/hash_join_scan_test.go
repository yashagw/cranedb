package query

import (
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

// newHashJoinTestTx spins up a fresh database and returns a transaction plus its
// table manager for building tables in hash join scan tests.
func newHashJoinTestTx(t *testing.T, testDir string) (*transaction.Transaction, *metadata.TableManager) {
	t.Helper()

	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	dpt := buffer.NewDirtyPageTable()
	bufferManager, err := buffer.NewManager(fileManager, logManager, dpt, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := buffer.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()
	transactionManager := transaction.NewTransactionManager(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	tx := transactionManager.BeginTransaction()
	require.NotNil(t, tx)

	tableManager := metadata.NewTableManager(true, tx)
	return tx, tableManager
}

// makeTableScan creates a table with the given schema and returns a scan over it.
func makeTableScan(t *testing.T, tm *metadata.TableManager, tx *transaction.Transaction, name string, sch *record.Schema) *table.TableScan {
	t.Helper()
	require.NoError(t, tm.CreateTable(name, sch, tx))
	layout, err := tm.GetLayout(name, tx)
	require.NoError(t, err)
	ts, err := table.NewTableScan(tx, layout, name)
	require.NoError(t, err)
	return ts
}

func TestHashJoinScanBasic(t *testing.T) {
	testDir := "/tmp/testdb_hashjoinscan_basic"
	defer os.RemoveAll(testDir)

	tx, tm := newHashJoinTestTx(t, testDir)
	defer tx.Commit()

	// Departments (build side): dept_id, dept_name
	deptSchema := record.NewSchema()
	deptSchema.AddIntField("dept_id")
	deptSchema.AddStringField("dept_name", 20)
	deptTS := makeTableScan(t, tm, tx, "Departments", deptSchema)

	departments := []struct {
		id   int
		name string
	}{{1, "CS"}, {2, "Math"}, {3, "Physics"}}
	require.NoError(t, deptTS.BeforeFirst())
	for _, d := range departments {
		require.NoError(t, deptTS.Insert())
		require.NoError(t, deptTS.SetInt("dept_id", d.id))
		require.NoError(t, deptTS.SetString("dept_name", d.name))
	}

	// Students (probe side): id, name, student_dept_id
	studentSchema := record.NewSchema()
	studentSchema.AddIntField("id")
	studentSchema.AddStringField("name", 20)
	studentSchema.AddIntField("student_dept_id")
	studentTS := makeTableScan(t, tm, tx, "Students", studentSchema)

	students := []struct {
		id     int
		name   string
		deptID int
	}{
		{1, "Alice", 1},
		{2, "Bob", 2},
		{3, "Charlie", 1},
		{4, "David", 3},
		{5, "Eve", 4}, // no matching department
	}
	require.NoError(t, studentTS.BeforeFirst())
	for _, s := range students {
		require.NoError(t, studentTS.Insert())
		require.NoError(t, studentTS.SetInt("id", s.id))
		require.NoError(t, studentTS.SetString("name", s.name))
		require.NoError(t, studentTS.SetInt("student_dept_id", s.deptID))
	}

	hjs, err := NewHashJoinScan(deptTS, studentTS, "dept_id", "student_dept_id", []string{"dept_id", "dept_name"})
	require.NoError(t, err)
	defer hjs.Close()

	// HasField spans both sides.
	assert.True(t, hjs.HasField("dept_name"))
	assert.True(t, hjs.HasField("name"))
	assert.False(t, hjs.HasField("missing"))

	type joinRow struct {
		student string
		dept    string
	}
	var got []joinRow
	for {
		hasNext, err := hjs.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		// Build-side field.
		deptName, err := hjs.GetString("dept_name")
		require.NoError(t, err)
		// Probe-side field.
		studentName, err := hjs.GetString("name")
		require.NoError(t, err)
		got = append(got, joinRow{studentName, deptName})
	}

	want := []joinRow{
		{"Alice", "CS"},
		{"Bob", "Math"},
		{"Charlie", "CS"},
		{"David", "Physics"},
		// Eve dropped: no matching department.
	}
	assert.ElementsMatch(t, want, got)
}

func TestHashJoinScanDuplicateKeys(t *testing.T) {
	// Many-to-many: two build rows and two probe rows share key 1,
	// so that key alone must yield 2x2 = 4 output rows.
	testDir := "/tmp/testdb_hashjoinscan_dupes"
	defer os.RemoveAll(testDir)

	tx, tm := newHashJoinTestTx(t, testDir)
	defer tx.Commit()

	leftSchema := record.NewSchema()
	leftSchema.AddIntField("k")
	leftSchema.AddStringField("lval", 20)
	leftTS := makeTableScan(t, tm, tx, "Left", leftSchema)

	left := []struct {
		k    int
		lval string
	}{{1, "L1a"}, {1, "L1b"}, {2, "L2"}}
	require.NoError(t, leftTS.BeforeFirst())
	for _, r := range left {
		require.NoError(t, leftTS.Insert())
		require.NoError(t, leftTS.SetInt("k", r.k))
		require.NoError(t, leftTS.SetString("lval", r.lval))
	}

	rightSchema := record.NewSchema()
	rightSchema.AddIntField("rk")
	rightSchema.AddStringField("rval", 20)
	rightTS := makeTableScan(t, tm, tx, "Right", rightSchema)

	right := []struct {
		rk   int
		rval string
	}{{1, "R1a"}, {1, "R1b"}, {3, "R3"}}
	require.NoError(t, rightTS.BeforeFirst())
	for _, r := range right {
		require.NoError(t, rightTS.Insert())
		require.NoError(t, rightTS.SetInt("rk", r.rk))
		require.NoError(t, rightTS.SetString("rval", r.rval))
	}

	hjs, err := NewHashJoinScan(leftTS, rightTS, "k", "rk", []string{"k", "lval"})
	require.NoError(t, err)
	defer hjs.Close()

	var pairs []string
	for {
		hasNext, err := hjs.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		lval, err := hjs.GetString("lval")
		require.NoError(t, err)
		rval, err := hjs.GetString("rval")
		require.NoError(t, err)
		pairs = append(pairs, lval+"-"+rval)
	}

	sort.Strings(pairs)
	// Only key 1 matches: {L1a,L1b} x {R1a,R1b} = 4 rows. Key 2 and 3 have no partner.
	assert.Equal(t, []string{"L1a-R1a", "L1a-R1b", "L1b-R1a", "L1b-R1b"}, pairs)
}

func TestHashJoinScanEmptyBuild(t *testing.T) {
	testDir := "/tmp/testdb_hashjoinscan_empty_build"
	defer os.RemoveAll(testDir)

	tx, tm := newHashJoinTestTx(t, testDir)
	defer tx.Commit()

	buildSchema := record.NewSchema()
	buildSchema.AddIntField("k")
	buildTS := makeTableScan(t, tm, tx, "Build", buildSchema)

	probeSchema := record.NewSchema()
	probeSchema.AddIntField("pk")
	probeTS := makeTableScan(t, tm, tx, "Probe", probeSchema)
	require.NoError(t, probeTS.BeforeFirst())
	for _, v := range []int{1, 2, 3} {
		require.NoError(t, probeTS.Insert())
		require.NoError(t, probeTS.SetInt("pk", v))
	}

	hjs, err := NewHashJoinScan(buildTS, probeTS, "k", "pk", []string{"k"})
	require.NoError(t, err)
	defer hjs.Close()

	hasNext, err := hjs.Next()
	require.NoError(t, err)
	assert.False(t, hasNext, "empty build side yields no rows")
}

func TestHashJoinScanEmptyProbe(t *testing.T) {
	testDir := "/tmp/testdb_hashjoinscan_empty_probe"
	defer os.RemoveAll(testDir)

	tx, tm := newHashJoinTestTx(t, testDir)
	defer tx.Commit()

	buildSchema := record.NewSchema()
	buildSchema.AddIntField("k")
	buildTS := makeTableScan(t, tm, tx, "Build", buildSchema)
	require.NoError(t, buildTS.BeforeFirst())
	for _, v := range []int{1, 2, 3} {
		require.NoError(t, buildTS.Insert())
		require.NoError(t, buildTS.SetInt("k", v))
	}

	probeSchema := record.NewSchema()
	probeSchema.AddIntField("pk")
	probeTS := makeTableScan(t, tm, tx, "Probe", probeSchema)

	hjs, err := NewHashJoinScan(buildTS, probeTS, "k", "pk", []string{"k"})
	require.NoError(t, err)
	defer hjs.Close()

	hasNext, err := hjs.Next()
	require.NoError(t, err)
	assert.False(t, hasNext, "empty probe side yields no rows")
}

func TestHashJoinScanStringKey(t *testing.T) {
	testDir := "/tmp/testdb_hashjoinscan_stringkey"
	defer os.RemoveAll(testDir)

	tx, tm := newHashJoinTestTx(t, testDir)
	defer tx.Commit()

	buildSchema := record.NewSchema()
	buildSchema.AddStringField("code", 10)
	buildSchema.AddStringField("label", 20)
	buildTS := makeTableScan(t, tm, tx, "Build", buildSchema)
	build := []struct{ code, label string }{{"A", "Apple"}, {"B", "Banana"}}
	require.NoError(t, buildTS.BeforeFirst())
	for _, r := range build {
		require.NoError(t, buildTS.Insert())
		require.NoError(t, buildTS.SetString("code", r.code))
		require.NoError(t, buildTS.SetString("label", r.label))
	}

	probeSchema := record.NewSchema()
	probeSchema.AddStringField("pcode", 10)
	probeSchema.AddIntField("qty")
	probeTS := makeTableScan(t, tm, tx, "Probe", probeSchema)
	probe := []struct {
		pcode string
		qty   int
	}{{"A", 5}, {"B", 7}, {"C", 9}}
	require.NoError(t, probeTS.BeforeFirst())
	for _, r := range probe {
		require.NoError(t, probeTS.Insert())
		require.NoError(t, probeTS.SetString("pcode", r.pcode))
		require.NoError(t, probeTS.SetInt("qty", r.qty))
	}

	hjs, err := NewHashJoinScan(buildTS, probeTS, "code", "pcode", []string{"code", "label"})
	require.NoError(t, err)
	defer hjs.Close()

	got := map[string]int{}
	for {
		hasNext, err := hjs.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		label, err := hjs.GetString("label")
		require.NoError(t, err)
		qty, err := hjs.GetInt("qty")
		require.NoError(t, err)
		got[label] = qty
	}
	assert.Equal(t, map[string]int{"Apple": 5, "Banana": 7}, got)
}

func TestHashJoinScanBoolKey(t *testing.T) {
	testDir := "/tmp/testdb_hashjoinscan_boolkey"
	defer os.RemoveAll(testDir)

	tx, tm := newHashJoinTestTx(t, testDir)
	defer tx.Commit()

	buildSchema := record.NewSchema()
	buildSchema.AddBoolField("flag")
	buildSchema.AddStringField("meaning", 20)
	buildTS := makeTableScan(t, tm, tx, "Build", buildSchema)
	build := []struct {
		flag    bool
		meaning string
	}{{true, "yes"}, {false, "no"}}
	require.NoError(t, buildTS.BeforeFirst())
	for _, r := range build {
		require.NoError(t, buildTS.Insert())
		require.NoError(t, buildTS.SetBool("flag", r.flag))
		require.NoError(t, buildTS.SetString("meaning", r.meaning))
	}

	probeSchema := record.NewSchema()
	probeSchema.AddBoolField("pflag")
	probeSchema.AddIntField("rowid")
	probeTS := makeTableScan(t, tm, tx, "Probe", probeSchema)
	probe := []struct {
		pflag bool
		rowid int
	}{{true, 1}, {false, 2}, {true, 3}}
	require.NoError(t, probeTS.BeforeFirst())
	for _, r := range probe {
		require.NoError(t, probeTS.Insert())
		require.NoError(t, probeTS.SetBool("pflag", r.pflag))
		require.NoError(t, probeTS.SetInt("rowid", r.rowid))
	}

	hjs, err := NewHashJoinScan(buildTS, probeTS, "flag", "pflag", []string{"flag", "meaning"})
	require.NoError(t, err)
	defer hjs.Close()

	byRow := map[int]string{}
	for {
		hasNext, err := hjs.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		rowid, err := hjs.GetInt("rowid")
		require.NoError(t, err)
		meaning, err := hjs.GetString("meaning")
		require.NoError(t, err)
		byRow[rowid] = meaning
	}
	assert.Equal(t, map[int]string{1: "yes", 2: "no", 3: "yes"}, byRow)
}

func TestHashJoinScanReIteration(t *testing.T) {
	// A second BeforeFirst must reuse the cached build table and produce
	// identical results — MaterializePlan-style consumers rely on re-scanning.
	testDir := "/tmp/testdb_hashjoinscan_reiter"
	defer os.RemoveAll(testDir)

	tx, tm := newHashJoinTestTx(t, testDir)
	defer tx.Commit()

	buildSchema := record.NewSchema()
	buildSchema.AddIntField("k")
	buildSchema.AddStringField("bval", 20)
	buildTS := makeTableScan(t, tm, tx, "Build", buildSchema)
	require.NoError(t, buildTS.BeforeFirst())
	for i := 1; i <= 3; i++ {
		require.NoError(t, buildTS.Insert())
		require.NoError(t, buildTS.SetInt("k", i))
		require.NoError(t, buildTS.SetString("bval", "b"))
	}

	probeSchema := record.NewSchema()
	probeSchema.AddIntField("pk")
	probeTS := makeTableScan(t, tm, tx, "Probe", probeSchema)
	require.NoError(t, probeTS.BeforeFirst())
	for i := 1; i <= 3; i++ {
		require.NoError(t, probeTS.Insert())
		require.NoError(t, probeTS.SetInt("pk", i))
	}

	hjs, err := NewHashJoinScan(buildTS, probeTS, "k", "pk", []string{"k", "bval"})
	require.NoError(t, err)
	defer hjs.Close()

	count := func() int {
		require.NoError(t, hjs.BeforeFirst())
		n := 0
		for {
			hasNext, err := hjs.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			n++
		}
		return n
	}

	first := count()
	second := count()
	assert.Equal(t, 3, first)
	assert.Equal(t, first, second, "re-iteration yields identical results")
}
