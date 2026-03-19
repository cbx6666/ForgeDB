package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"monolithdb"
)

func main() {
	dir := "demo-data"
	if len(os.Args) > 1 && strings.TrimSpace(os.Args[1]) != "" {
		dir = os.Args[1]
	}

	db, err := monolithdb.Open(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	fmt.Printf("MonolithDB demo\n")
	fmt.Printf("dir: %s\n", dir)
	printHelp()

	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			fmt.Println()
			return
		}

		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}

		args := strings.Fields(line)
		cmd := strings.ToLower(args[0])

		switch cmd {
		case "h", "help":
			printHelp()

		case "p", "put":
			if len(args) < 3 {
				fmt.Println("usage: p <key> <value>")
				continue
			}
			if err := db.Put(args[1], []byte(strings.Join(args[2:], " "))); err != nil {
				fmt.Printf("put err: %v\n", err)
				continue
			}
			fmt.Println("ok")

		case "g", "get":
			if len(args) != 2 {
				fmt.Println("usage: g <key>")
				continue
			}
			v, ok, err := db.Get(args[1])
			if err != nil {
				fmt.Printf("get err: %v\n", err)
				continue
			}
			if !ok {
				fmt.Println("(not found)")
				continue
			}
			fmt.Printf("%s\n", string(v))

		case "d", "del", "delete":
			if len(args) != 2 {
				fmt.Println("usage: d <key>")
				continue
			}
			if err := db.Delete(args[1]); err != nil {
				fmt.Printf("delete err: %v\n", err)
				continue
			}
			fmt.Println("ok")

		case "s", "scan":
			start := ""
			end := ""
			if len(args) >= 2 {
				start = args[1]
			}
			if len(args) >= 3 {
				end = args[2]
			}
			entries, err := db.Scan(start, end)
			if err != nil {
				fmt.Printf("scan err: %v\n", err)
				continue
			}
			if len(entries) == 0 {
				fmt.Println("(empty)")
				continue
			}
			for _, ent := range entries {
				fmt.Printf("%s=%s\n", ent.Key, string(ent.Value))
			}

		case "f", "flush":
			if err := db.Flush(); err != nil {
				fmt.Printf("flush err: %v\n", err)
				continue
			}
			fmt.Println("ok")

		case "c", "compact":
			if err := db.RequestCompaction(); err != nil {
				fmt.Printf("compact err: %v\n", err)
				continue
			}
			fmt.Println("ok (background)")

		case "i", "iter":
			start := ""
			end := ""
			if len(args) >= 2 {
				start = args[1]
			}
			if len(args) >= 3 {
				end = args[2]
			}
			it, err := db.NewIterator(start, end)
			if err != nil {
				fmt.Printf("iter err: %v\n", err)
				continue
			}
			defer func() { _ = it.Close() }()

			if !it.Valid() {
				fmt.Println("(empty)")
				continue
			}
			for it.Valid() {
				ent := it.Entry()
				fmt.Printf("%s=%s\n", ent.Key, string(ent.Value))
				if err := it.Next(); err != nil {
					fmt.Printf("iter next err: %v\n", err)
					break
				}
			}

		case "ls", "files":
			if err := printFiles(dir); err != nil {
				fmt.Printf("files err: %v\n", err)
			}

		case "q", "quit", "exit":
			return

		default:
			fmt.Println("unknown command, use `help`")
		}
	}
}

func printHelp() {
	fmt.Println("commands:")
	fmt.Println("  p <k> <v>    put")
	fmt.Println("  g <k>        get")
	fmt.Println("  d <k>        delete")
	fmt.Println("  s [a] [b]    scan [a, b)")
	fmt.Println("  i [a] [b]    iterator [a, b)")
	fmt.Println("  f            flush")
	fmt.Println("  c            compact")
	fmt.Println("  ls           show manifest/wal/sst files")
	fmt.Println("  q            quit")
}

func printFiles(root string) error {
	var paths []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			paths = append(paths, rel+string(os.PathSeparator))
			return nil
		}
		paths = append(paths, fmt.Sprintf("%s (%dB)", rel, info.Size()))
		return nil
	})
	if err != nil {
		return err
	}

	sort.Strings(paths)
	if len(paths) == 0 {
		fmt.Println("(no files)")
		return nil
	}
	for _, p := range paths {
		fmt.Println(p)
	}
	return nil
}
