package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rakyll/magicmime"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type FileInfo struct {
	contentMD5 string
	pathMD5    string
	path       string
	mimeType   string
	size       int
}

func getFileInfo(path string) (*FileInfo, error) {
	// guess file mime type
	mimeType, err := magicmime.TypeByFile(path)
	if err != nil {
		log.Printf("FAILED to guess type for file %v, error: %v\n", path, err)
		mimeType = "unknown"
	}

	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	// get file content md5
	size := 0
	buf := make([]byte, 1024*128)
	h := md5.New()
	for {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			break
		}
		size = size + n
		_, err = h.Write(buf[:n])
		if err != nil {
			return nil, err
		}
	}
	contentMD5 := hex.EncodeToString(h.Sum(nil))

	// get path md5
	data := md5.Sum([]byte(path))
	pathMD5 := hex.EncodeToString(data[:])

	// return file info struct
	fi := FileInfo{contentMD5, pathMD5, path, mimeType, size}

	return &fi, nil
}

var instStmtMap = map[int]*sql.Stmt{}

func getSaveEntriesDbStmt(db *sql.DB, n int) *sql.Stmt {
	if n < 1 {
		panic(fmt.Sprintf("cannot create mysql statement with %v args", n))
	}
	stmt, ok := instStmtMap[n]
	if ok == true {
		return stmt
	}

	p := make([]string, n)
	for i := 0; i < n; i++ {
		p[i] = "(?,?,?,?,?)"
	}
	sql := fmt.Sprintf(
		"insert ignore into entry (content_md5,path_md5,path,size,mime_type) values %v",
		strings.Join(p, ","))
	stmt, err := db.Prepare(sql)
	if err != nil {
		panic(err)
	}
	instStmtMap[n] = stmt
	return stmt
}

func saveEntries(db *sql.DB, cnt int, args []interface{}) {
	stmt := getSaveEntriesDbStmt(db, cnt)
	r, err := stmt.Exec(args...)
	if err != nil {
		panic(err)
	}
	rowsAffected, err := r.RowsAffected()
	if err != nil {
		panic(err)
	}
	log.Printf("saved %v out of %v file entries into db\n", rowsAffected, cnt)
}

func scanDir(dirPath string, db *sql.DB) {
	args := []interface{}{}
	fileCnt := 0
	walkFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if info != nil && info.IsDir() {
				log.Printf("skipping directory %v due to error %v\n", path, err)
				return filepath.SkipDir
			} else {
				return nil
			}
		}
		if info.IsDir() {
			return nil
		}
		if strings.ToLower(filepath.Base(path)) == "thumbs.db" {
			return nil
		}
		fi, err := getFileInfo(path)
		if err == nil {
			args = append(args, fi.contentMD5, fi.pathMD5, fi.path, fi.size, fi.mimeType)
			fileCnt = fileCnt + 1
			// bulk save to db
			if fileCnt >= 1000 {
				saveEntries(db, fileCnt, args)
				// reset counter and args
				fileCnt = 0
				args = []interface{}{}
			}
		} else {
			log.Printf("failed to process file %v, error: %v\n", path, err)
		}
		return nil
	}
	filepath.Walk(dirPath, walkFunc)
	// save what is left to db
	if fileCnt > 0 {
		saveEntries(db, fileCnt, args)
	}
}

type DbConf struct {
	protocol string
	address  string
	username string
	password string
	database string
}

func getDbDsn(conf *DbConf) string {
	return fmt.Sprintf("%v:%v@%v(%v)/%v?parseTime=true",
		conf.username,
		conf.password,
		conf.protocol,
		conf.address,
		conf.database)
}

func parseCmdArgs() (*DbConf, []string) {
	protocol := flag.String("protocol", "tcp", "DB connection protocol: unix/tcp/...")
	address := flag.String("addr", "localhost:3306", "DB server address")
	username := flag.String("username", "filemanager", "DB access username")
	password := flag.String("password", "filemanager", "DB access password")
	database := flag.String("database", "my_media_file", "database name")

	flag.Parse()

	conf := &DbConf{*protocol, *address, *username, *password, *database}

	log.Printf("Database DSN: %v\n", getDbDsn(conf))

	return conf, flag.Args()
}

func getDbConnection(conf *DbConf) *sql.DB {
	db, err := sql.Open("mysql", getDbDsn(conf))
	if err != nil {
		panic(nil)
	}
	return db
}

func scan(dirs []string, db *sql.DB) {
	// open libmagic database file
	if err := magicmime.Open(magicmime.MAGIC_MIME_TYPE | magicmime.MAGIC_SYMLINK | magicmime.MAGIC_ERROR); err != nil {
		log.Fatal(err)
	}
	defer magicmime.Close()

	for _, dirPath := range dirs {
		scanDir(dirPath, db)
	}
}

func getExtFromType(typ string) string {
	switch typ {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "video/mpeg":
		return ".mpg"
	case "video/mp4":
		return ".mp4"
	}
	return ".dat"
}

var updStsStmtMap = map[int]*sql.Stmt{}

func getUpdateStatusStmt(db *sql.DB, n int) *sql.Stmt {
	stmt, ok := updStsStmtMap[n]
	if ok == true {
		return stmt
	}
	p := make([]string, n)
	for i := 0; i < n; i++ {
		p[i] = "(?,?)"
	}
	sql := fmt.Sprintf(
		"insert into entry (id,status) values %v on duplicate key update status=values(status)",
		strings.Join(p, ","))
	stmt, err := db.Prepare(sql)
	if err != nil {
		panic(err)
	}
	updStsStmtMap[n] = stmt
	return stmt
}

func updateStatus(db *sql.DB, ids []int, sts int) {
	var args []interface{}
	for _, id := range ids {
		args = append(args, id, sts)
	}
	stmt := getUpdateStatusStmt(db, len(ids))
	r, err := stmt.Exec(args...)
	if err != nil {
		panic(err)
	}
	rowsAffected, err := r.RowsAffected()
	if err != nil {
		panic(err)
	}
	log.Printf("updated %v file entries' status in db\n", rowsAffected/2)
}

func link(args []string, db *sql.DB) {
	if args == nil || len(args) < 1 {
		log.Fatal(errors.New("Missing link target root dir!"))
	}
	root := args[0]
	sizeThreshold := int64(1024 * 1024 * 1024 * 10) // 10GB
	if len(args) > 1 {
		gb, err := strconv.Atoi(args[1])
		if err != nil {
			panic(err)
		}
		sizeThreshold = int64(1024 * 1024 * 1024 * gb)
	}

	// query db to get ready files
	sql := "select id,content_md5,mime_type,path,size from entry where status=? order by id asc"
	rows, err := db.Query(sql, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var processed []int
	var totalSize int64
	var cnt = 0

	var id int
	var contentMD5 string
	var mimeType string
	var path string
	var size int64

	for rows.Next() {
		if err = rows.Scan(&id, &contentMD5, &mimeType, &path, &size); err != nil {
			log.Fatal(err)
		}
		// create dir
		hashDir := fmt.Sprintf("%v/%v", contentMD5[0:2], contentMD5[2:4])
		var dir string
		if strings.HasPrefix(mimeType, "image/") {
			dir = filepath.Join(root, "Pictures", hashDir)
		} else if strings.HasPrefix(mimeType, "video/") {
			dir = filepath.Join(root, "Videos", hashDir)
		} else {
			dir = filepath.Join(root, "Documents", hashDir)
		}
		if err = os.MkdirAll(dir, 0755); err != nil {
			panic(err)
		}
		// create symlink
		ext := filepath.Ext(path)
		if ext == "" {
			ext = getExtFromType(mimeType)
		}
		ext = strings.ToLower(ext)
		filename := fmt.Sprintf("%v%v", contentMD5, ext)
		if err = os.Symlink(path, filepath.Join(dir, filename)); err != nil {
			panic(err)
		}

		processed = append(processed, id)
		totalSize = totalSize + size
		cnt = cnt + 1
		if totalSize > sizeThreshold {
			break
		}
		if len(processed) >= 1000 {
			updateStatus(db, processed, 1)
			processed = []int{}
		}
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
	if len(processed) > 0 {
		updateStatus(db, processed, 1)
	}
	log.Printf("Linked %v files, size: %v bytes", cnt, totalSize)
}

func main() {
	dbConf, args := parseCmdArgs()
	db := getDbConnection(dbConf)
	switch args[0] {
	case "scan":
		scan(args[1:], db)
		return
	case "link":
		link(args[1:], db)
		return
	}
	log.Fatal(fmt.Sprintf("unsupported subcommand: %v", args[0]))
}
