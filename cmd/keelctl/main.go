// Command keelctl is a small command line front end for a keelstore
// directory. It exists so that the engine can be poked at without writing a
// program: put a few keys, scan a range, look at the level layout, force a
// compaction and watch the files move.
//
// Usage:
//
//	keelctl [-dir DIR] [-sync] COMMAND [args]
//
//	put KEY VALUE
//	get KEY
//	delete KEY
//	scan [-start KEY] [-end KEY] [-limit N]
//	stats
//	compact
//
// Global flags come before the command; a command's own flags come after
// it. Only one process may hold a store open at a time, so keelctl opens
// the directory, does its work and closes it again on every invocation.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/aminyx/keelstore"
)

const usage = `keelctl - inspect and modify a keelstore directory

usage:
  keelctl [-dir DIR] [-sync] COMMAND [args]

commands:
  put KEY VALUE                              store a value
  get KEY                                    read a value
  delete KEY                                 write a tombstone
  scan [-start KEY] [-end KEY] [-limit N]    walk a key range in order
  stats                                      report the shape of the tree
  compact                                    merge every level downwards

global flags:
  -dir DIR    store directory (default "keeldata")
  -sync       fsync the write-ahead log on every write
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "keelctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	global := flag.NewFlagSet("keelctl", flag.ContinueOnError)
	global.SetOutput(os.Stderr)
	global.Usage = func() { fmt.Fprint(os.Stderr, usage) }

	dir := global.String("dir", "keeldata", "store directory")
	sync := global.Bool("sync", false, "fsync the write-ahead log on every write")

	if err := global.Parse(args); err != nil {
		return err
	}
	rest := global.Args()
	if len(rest) == 0 {
		global.Usage()
		return errors.New("no command given")
	}
	cmd, cmdArgs := rest[0], rest[1:]

	opts := keelstore.Options{SyncWrites: *sync}
	db, err := keelstore.Open(*dir, &opts)
	if err != nil {
		return err
	}
	defer db.Close()

	switch cmd {
	case "put":
		if len(cmdArgs) != 2 {
			return errors.New("put needs a key and a value")
		}
		if err := db.Put([]byte(cmdArgs[0]), []byte(cmdArgs[1])); err != nil {
			return err
		}
		fmt.Printf("put %s\n", cmdArgs[0])

	case "get":
		if len(cmdArgs) != 1 {
			return errors.New("get needs a key")
		}
		v, err := db.Get([]byte(cmdArgs[0]))
		if errors.Is(err, keelstore.ErrNotFound) {
			fmt.Printf("%s: not found\n", cmdArgs[0])
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", v)

	case "delete", "del":
		if len(cmdArgs) != 1 {
			return errors.New("delete needs a key")
		}
		if err := db.Delete([]byte(cmdArgs[0])); err != nil {
			return err
		}
		fmt.Printf("deleted %s\n", cmdArgs[0])

	case "scan":
		return runScan(db, cmdArgs)

	case "stats":
		fmt.Print(db.Stats())

	case "compact":
		if err := db.Compact(); err != nil {
			return err
		}
		s := db.Stats()
		fmt.Printf("compacted: %d compaction(s), layout %s\n", s.Compactions, layoutOf(s))

	default:
		global.Usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

func runScan(db *keelstore.DB, args []string) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	start := fs.String("start", "", "inclusive lower bound")
	end := fs.String("end", "", "exclusive upper bound")
	limit := fs.Int("limit", 0, "stop after N keys (0 means no limit)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var opts keelstore.IterOptions
	if *start != "" {
		opts.Start = []byte(*start)
	}
	if *end != "" {
		opts.End = []byte(*end)
	}

	it := db.NewIterator(&opts)
	defer it.Close()

	n := 0
	for it.Next() {
		fmt.Printf("%s\t%s\n", it.Key(), it.Value())
		n++
		if *limit > 0 && n >= *limit {
			break
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	fmt.Printf("(%d key%s)\n", n, plural(n))
	return nil
}

func layoutOf(s keelstore.Stats) string {
	levels := make([]string, 0, len(s.SSTablesPerLevel))
	for i, n := range s.SSTablesPerLevel {
		if n == 0 {
			continue
		}
		levels = append(levels, fmt.Sprintf("L%d=%d", i, n))
	}
	if len(levels) == 0 {
		return "empty"
	}
	return strings.Join(levels, " ")
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
