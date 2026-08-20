package keelstore

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Environment variables that turn a re-run of this test binary into the
// child process the crash tests kill.
const (
	crashDirEnv  = "KEELSTORE_CRASH_DIR"
	crashModeEnv = "KEELSTORE_CRASH_MODE"

	crashAckName  = "acked.txt"
	crashAckWidth = 10
	crashTornKey  = "torn-record-key"
	crashAcked    = 100
)

// crashForTest drops the file handles without any of the tidy-up Close
// does: no final manifest write, no flush, no clean shutdown. What is left
// in the directory is what a killed process leaves behind.
func (db *DB) crashForTest() {
	db.closed.Store(true)
	db.bgCancel()
	db.mu.Lock()
	db.closing = true
	db.flushCond.Broadcast()
	db.mu.Unlock()
	db.bgWG.Wait()
	if db.wal != nil {
		db.wal.Close()
	}
	db.cache.close()
}

func TestRecoveryAfterAnUncleanShutdown(t *testing.T) {
	opts := smallOptions()
	dir := t.TempDir()

	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		mustPut(t, db, key(i), value(i))
	}
	for i := 0; i < 500; i += 5 {
		mustDelete(t, db, key(i))
	}
	db.crashForTest() // no Close: the manifest is stale and the log is hot

	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatalf("Open after a crash: %v", err)
	}
	defer db.Close()

	for i := 0; i < 500; i++ {
		if i%5 == 0 {
			mustMissing(t, db, key(i))
		} else {
			mustGet(t, db, key(i), value(i))
		}
	}
	// The recovered store must be writable again.
	mustPut(t, db, "after-recovery", "ok")
	mustGet(t, db, "after-recovery", "ok")
}

func TestRecoveryDiscardsATornTailButKeepsEverythingBefore(t *testing.T) {
	opts := smallOptions()
	opts.SyncWrites = true
	dir := t.TempDir()

	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		mustPut(t, db, key(i), value(i))
	}
	// The next record reaches the disk only partially, as it would if the
	// machine lost power halfway through the write.
	db.injectTornTail(9)
	if err := db.Put([]byte(crashTornKey), []byte("never landed")); err != nil {
		t.Fatal(err)
	}
	db.crashForTest()

	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatalf("Open after a torn write: %v", err)
	}
	defer db.Close()

	for i := 0; i < 200; i++ {
		mustGet(t, db, key(i), value(i))
	}
	mustMissing(t, db, crashTornKey)
	if dropped := db.Stats().WALBytesDropped; dropped != 9 {
		t.Fatalf("dropped %d bytes of log, want the 9 torn ones", dropped)
	}
}

func TestRecoveryReplaysAcrossSeveralLogFiles(t *testing.T) {
	opts := smallOptions()
	opts.MemtableSize = 1 << 10 // rotate the log often
	dir := t.TempDir()

	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		mustPut(t, db, key(i), value(i))
	}
	logs, err := listNumberedFiles(dir, walExt)
	if err != nil {
		t.Fatal(err)
	}
	db.crashForTest()
	t.Logf("crashed with %d log file(s) on disk", len(logs))

	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	for i := 0; i < 400; i++ {
		mustGet(t, db, key(i), value(i))
	}
}

func TestRecoveryLeavesNoLogFilesBehind(t *testing.T) {
	opts := smallOptions()
	dir := t.TempDir()
	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		mustPut(t, db, key(i), value(i))
	}
	db.crashForTest()

	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	logs, err := listNumberedFiles(dir, walExt)
	if err != nil {
		t.Fatal(err)
	}
	// Recovery folds every old log into an SSTable and starts exactly one
	// fresh log for the new memtable.
	if len(logs) != 1 {
		t.Fatalf("%d log files after recovery, want exactly 1", len(logs))
	}
	if db.Stats().SSTables == 0 {
		t.Fatal("recovery did not turn the replayed data into a table")
	}
}

// -------------------------------------------------------------------------
// the real thing: a child process that is killed while it writes
// -------------------------------------------------------------------------

// TestCrashChildProcess is not a test. It is the body of the child process
// that TestCrashRecoveryKilledChild and TestCrashRecoveryKilledChildWithTornTail
// launch by re-running this test binary. When the environment variable is
// not set it does nothing.
func TestCrashChildProcess(t *testing.T) {
	dir := os.Getenv(crashDirEnv)
	if dir == "" {
		t.Skip("child process helper; driven by the crash recovery tests")
	}
	runCrashChild(dir, os.Getenv(crashModeEnv))
}

func runCrashChild(dir, mode string) {
	opts := smallOptions()
	opts.SyncWrites = true // acknowledged means durable

	db, err := Open(dir, &opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "child open:", err)
		os.Exit(2)
	}

	ack, err := os.OpenFile(filepath.Join(dir, crashAckName), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "child ack file:", err)
		os.Exit(2)
	}
	record := func(i int) {
		if _, err := ack.WriteAt([]byte(fmt.Sprintf("%*d", crashAckWidth, i)), 0); err != nil {
			os.Exit(2)
		}
		if err := ack.Sync(); err != nil {
			os.Exit(2)
		}
	}

	for i := 0; i < crashAcked; i++ {
		if err := db.Put([]byte(key(i)), []byte(value(i))); err != nil {
			fmt.Fprintln(os.Stderr, "child put:", err)
			os.Exit(2)
		}
		record(i)
	}

	if mode == "torn" {
		// Everything above is fsynced. This last record only partially
		// reaches the disk, which is what a power cut looks like.
		db.injectTornTail(11)
		if err := db.Put([]byte(crashTornKey), []byte("this record never landed")); err != nil {
			os.Exit(2)
		}
		fmt.Println("READY")
		os.Stdout.Sync()
		select {} // wait to be killed
	}

	// Streaming mode: keep writing forever, so the kill lands somewhere in
	// the middle of the write path rather than at a tidy boundary.
	fmt.Println("READY")
	os.Stdout.Sync()
	for i := crashAcked; ; i++ {
		if err := db.Put([]byte(key(i)), []byte(value(i))); err != nil {
			os.Exit(2)
		}
		record(i)
	}
}

// spawnAndKillChild runs this test binary as a child process against dir,
// waits for it to report that it is ready, and kills it.
func spawnAndKillChild(t *testing.T, dir, mode string) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChildProcess$", "-test.timeout=120s")
	cmd.Env = append(os.Environ(), crashDirEnv+"="+dir, crashModeEnv+"="+mode)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the child: %v", err)
	}

	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.TrimSpace(scanner.Text()) == "READY" {
				ready <- true
				return
			}
		}
		ready <- false
	}()

	select {
	case ok := <-ready:
		if !ok {
			cmd.Process.Kill()
			cmd.Wait()
			t.Fatal("the child exited before it was ready")
		}
	case <-time.After(90 * time.Second):
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal("the child never reported ready")
	}

	if mode == "stream" {
		// Let it get well into the write loop before pulling the plug.
		time.Sleep(150 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("killing the child: %v", err)
	}
	cmd.Wait() // the exit status of a killed process is not interesting
}

func readAckedIndex(t *testing.T, dir string) int {
	t.Helper()
	buf, err := os.ReadFile(filepath.Join(dir, crashAckName))
	if err != nil {
		t.Fatalf("reading the acknowledgement file: %v", err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(buf)))
	if err != nil {
		t.Fatalf("acknowledgement file holds %q: %v", buf, err)
	}
	return n
}

// The point of the whole exercise: a process is killed while it is writing,
// and every write it had acknowledged is still there afterwards.
func TestCrashRecoveryKilledChild(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a child process")
	}
	dir := t.TempDir()
	spawnAndKillChild(t, dir, "stream")

	acked := readAckedIndex(t, dir)
	if acked < crashAcked-1 {
		t.Fatalf("the child only acknowledged %d writes before it died", acked+1)
	}
	t.Logf("child was killed after acknowledging %d writes", acked+1)

	opts := smallOptions()
	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatalf("Open after the kill: %v", err)
	}
	defer db.Close()

	for i := 0; i <= acked; i++ {
		mustGet(t, db, key(i), value(i))
	}
	// The store is fully usable again.
	mustPut(t, db, "after-crash", "ok")
	mustGet(t, db, "after-crash", "ok")

	it := db.NewIterator(nil)
	defer it.Close()
	keys, _ := collect(t, it)
	if len(keys) < acked+1 {
		t.Fatalf("iteration found %d keys, fewer than the %d acknowledged", len(keys), acked+1)
	}
}

// Same, but the process dies with a half-written record at the end of the
// log. The torn record must be detected by its checksum and discarded,
// without taking anything before it down.
func TestCrashRecoveryKilledChildWithTornTail(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a child process")
	}
	dir := t.TempDir()
	spawnAndKillChild(t, dir, "torn")

	acked := readAckedIndex(t, dir)
	if acked != crashAcked-1 {
		t.Fatalf("the child acknowledged %d writes, want %d", acked+1, crashAcked)
	}

	opts := smallOptions()
	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatalf("Open after the kill: %v", err)
	}
	defer db.Close()

	for i := 0; i < crashAcked; i++ {
		mustGet(t, db, key(i), value(i))
	}
	mustMissing(t, db, crashTornKey)

	if dropped := db.Stats().WALBytesDropped; dropped != 11 {
		t.Fatalf("recovery dropped %d bytes, want the 11 torn ones", dropped)
	}
	t.Logf("recovered %d acknowledged writes and discarded an %d byte torn record",
		crashAcked, db.Stats().WALBytesDropped)

	mustPut(t, db, "after-crash", "ok")
	mustGet(t, db, "after-crash", "ok")
}
