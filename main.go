package main

import (
	//"filemanager/storage"
	"filemanager/storage/fs"

	"github.com/rs/zerolog"
	"github.com/yizha/go/logging"
	"github.com/yizha/go/logging/writer/stdout"
)

func main() {
	logging.SetupGlobalConf(
		logging.DefaultGlobalConf().
			SetTimestampFormat("2006-01-02T15:04:05.999999"))
	logging.SetupDefaults(zerolog.InfoLevel, true, false, stdout.New())
	lg := logging.GetLogger("main")

	src, err := fs.New("/tmp/source", 4, 4, lg)
	if err != nil {
		panic(err.Error())
	}
	lg.Info().Msg("source storage initialized.")

	dst, err := fs.New("/tmp/destination", 4, 4, lg)
	if err != nil {
		panic(err.Error())
	}
	lg.Info().Msg("destination storage initialized.")

	sr := src.Scan()
	dr := dst.Store(sr.Blob())

	lg.Info().Msg("copying from source to destination ...")

	<-sr.Done()
	<-dr.Done()

	lg.Info().Msgf("source result: %s", sr.JSONStr())
	lg.Info().Msgf("destination result: %s", dr.JSONStr())
}
