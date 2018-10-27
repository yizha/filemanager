package filesystem

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"sync"

	"filemanager/blob"
	"filemanager/meta"
	"filemanager/util"

	"github.com/rs/zerolog"
)

var fileCmdPath string

func init() {
	fileCmdPath = lookupFileCmd()
}

// look up 'file' command, panic if not found
func lookupFileCmd() string {
	path, err := exec.LookPath("file")
	if err != nil {
		panic(err.Error())
	}
	return path
}

// example:
//   text/plain; charset=us-ascii
func parseMimeString(s string) *MimeType {
	parts := strings.SplitN(s, ";", 2)
	if len(parts) < 2 {
		return nil
	}
	mime := strings.SplitN(parts[0], "/", 2)
	if len(mime) < 2 {
		return nil
	}
	charset := strings.SplitN(parts[1], "=", 2)
	if len(charset) < 2 {
		return nil
	}
	mimeType := strings.TrimSpace(mime[0])
	mimeSubtype := strings.TrimSpace(mime[1])
	encoding := strings.TrimSpace(charset[1])
	return &MimeType{
		Type:     mimeType,
		Subtype:  mimeSubtype,
		Encoding: encoding,
	}
}

func parseMimeLine(path2meta map[string]*meta.BlobMeta, line string) (*meta.BlobMeta, *MimeType) {
	idx := strings.Index(line, ":")
	if idx == -1 {
		return nil, nil
	}
	bm, ok := path2meta[line[0:idx]]
	if !ok {
		return nil, nil
	}
	return bm, parseMimeString(strings.TrimSpace(line[idx+1:]))
}

func parseDescLine(path2meta map[string]*meta.BlobMeta, line string) (*meta.BlobMeta, string) {
	idx := strings.Index(line, ":")
	if idx == -1 {
		return nil, ""
	}
	bm, ok := path2meta[line[0:idx]]
	if !ok {
		return nil, ""
	}
	return bm, strings.TrimSpace(line[idx+1:])
}

func detectWithFileCmd(
	path2meta map[string]*meta.BlobMeta,
	inputFilePath string,
	outCh chan *meta.MetaExtractResult) {

	// detect mime info
	outBytes, err := exec.Command(fileCmdPath, "-p", "--mime", "-f", inputFilePath).Output()
	if err != nil {
		outCh <- meta.NewMetaExtractErr(err)
	} else {
		out := string(outBytes)
		for _, line := range strings.Split(out, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			bm, mt := parseMimeLine(path2meta, line)
			if bm == nil {
				ex := fmt.Errorf("failed to match path for line %s", line)
				outCh <- meta.NewMetaExtractErr(ex)
				continue
			}
			if mt == nil {
				ex := fmt.Errorf("failed to parse mime info for line %s", line)
				outCh <- meta.NewMetaExtractErr(ex)
				continue
			}
			bm.Add("filetype-mime-type", meta.StringValue(mt.Type))
			bm.Add("filetype-mime-subtype", meta.StringValue(mt.Subtype))
			bm.Add("filetype-mime-encoding", meta.StringValue(mt.Encoding))
		}
	}

	// detect file description
	outBytes, err = exec.Command(fileCmdPath, "-p", "-f", inputFilePath).Output()
	if err != nil {
		outCh <- meta.NewMetaExtractErr(err)
	} else {
		out := string(outBytes)
		for _, line := range strings.Split(out, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			bm, desc := parseDescLine(path2meta, line)
			if bm == nil {
				ex := fmt.Errorf("failed to match path for line %s", line)
				outCh <- meta.NewMetaExtractErr(ex)
				continue
			}
			if desc != "" {
				bm.Add("filetype-description", meta.StringValue(desc))
			}
		}
	}
}

func writeFileList(f *os.File, path2meta map[string]*meta.BlobMeta) error {
	defer f.Close()
	for path := range path2meta {
		_, err := f.WriteString(fmt.Sprintf("%s\n", path))
		if err != nil {
			return err
		}
	}
	return nil
}

func detect(
	workDir string,
	files []*blob.FileBlob,
	outCh chan *meta.MetaExtractResult,
	wg *sync.WaitGroup) {

	defer wg.Done()

	// collect files info
	cnt := len(files)
	path2meta := make(map[string]*meta.BlobMeta, cnt)
	for _, f := range files {
		path := f.Path()
		hash := f.Hash().String()
		bm := meta.NewBlobMeta(hash)
		mt := MapName2Mime(f.Name())
		fname := f.Name()
		bm.Add("filename", meta.StringValue(fname))
		bm.Add("fileext", meta.StringValue(util.FileExt(fname)))
		bm.Add("fileext-mime-type", meta.StringValue(mt.Type))
		bm.Add("fileext-mime-subtype", meta.StringValue(mt.Subtype))
		path2meta[path] = bm
	}

	// touch a tmp file to write all file path
	tmpfile, err := ioutil.TempFile(workDir, "detect-file-mime-")
	if err != nil {
		outCh <- meta.NewMetaExtractErr(err)
	} else {

		tmpfilepath := tmpfile.Name()
		defer os.Remove(tmpfilepath)

		// write to the tmp file
		if err := writeFileList(tmpfile, path2meta); err != nil {
			outCh <- meta.NewMetaExtractErr(err)

		} else {
			// successfully created the list file, call "file" command to
			// detect file mime info
			detectWithFileCmd(path2meta, tmpfilepath, outCh)
		}
	}

	for _, bm := range path2meta {
		outCh <- meta.NewMetaExtractResult(bm)
	}
}

func dispatch(
	workDir string,
	batch int,
	inCh chan *blob.FileBlob,
	outCh chan *meta.MetaExtractResult) {

	wg := &sync.WaitGroup{}
	files := make([]*blob.FileBlob, batch, batch)
	i := 0
	for f := range inCh {
		files[i] = f
		i++
		if i >= batch {
			wg.Add(1)
			go detect(workDir, files[0:i], outCh, wg)
			files = make([]*blob.FileBlob, batch, batch)
			i = 0
		}
	}
	if i > 0 {
		wg.Add(1)
		go detect(workDir, files[0:i], outCh, wg)
	}
	wg.Wait()
	close(outCh)
}

func output(
	inCh chan *meta.MetaExtractResult,
	outCh chan *meta.BlobMeta,
	lg *zerolog.Logger) {

	for r := range inCh {
		if r.Error == nil {
			outCh <- r.BlobMeta
		} else {
			lg.Warn().
				Err(r.Error).
				Msg("detect file type")
		}
	}

	close(outCh)
}

func DetectMimeType(
	workDir string,
	batch int,
	inCh chan *blob.FileBlob,
	lg *zerolog.Logger) chan *meta.BlobMeta {

	midCh := make(chan *meta.MetaExtractResult)
	outCh := make(chan *meta.BlobMeta)

	go output(midCh, outCh, lg)
	go dispatch(workDir, batch, inCh, midCh)

	return outCh
}
