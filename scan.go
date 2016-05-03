package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rakyll/magicmime"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileInfo struct {
	contentMD5 string
	pathMD5    string
	path       string
	mimeType   string
	size       int64
	modTime    time.Time
}

func getFileInfo(path string) (*FileInfo, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

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
	buf := make([]byte, 8192)
	h := md5.New()
	for {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			break
		}
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
	size := fileInfo.Size()
	modTime := fileInfo.ModTime()
	fi := FileInfo{contentMD5, pathMD5, path, mimeType, size, modTime}

	return &fi, nil
}

var stmtMap = map[int]*sql.Stmt{}

func getSaveEntriesDbStmt(db *sql.DB, n int) *sql.Stmt {
	if n < 1 {
		panic(fmt.Sprintf("cannot create mysql statement with %v args", n))
	}
	stmt, ok := stmtMap[n]
	if ok == true {
		return stmt
	}

	p := make([]string, n)
	for i := 0; i < n; i++ {
		p[i] = "(?,?,?,?,?,?)"
	}
	sql := fmt.Sprintf(
		"insert ignore into entry (content_md5,path_md5,path,size,mime_type,mod_time) values %v",
		strings.Join(p, ","))
	stmt, err := db.Prepare(sql)
	if err != nil {
		panic(err)
	}
	stmtMap[n] = stmt
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
	log.Printf("insert into entry: %v rows affected\n", rowsAffected)
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
		fi, err := getFileInfo(path)
		if err == nil {
			args = append(args, fi.contentMD5, fi.pathMD5, fi.path, fi.size, fi.mimeType, fi.modTime)
			fileCnt = fileCnt + 1
			//			log.Println(fi.contentMD5, fi.pathMD5,
			//			fi.size, fi.mimeType, fi.modTime)
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

func main() {
	dbConf, args := parseCmdArgs()
	db := getDbConnection(dbConf)
	scan(args, db)
}
