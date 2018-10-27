package logging

import (
	"os"

	"github.com/rs/zerolog"
)

var logger zerolog.Logger

func init() {
	zerolog.TimestampFieldName = "timestamp"
	zerolog.LevelFieldName = "level"
	zerolog.MessageFieldName = "message"
	zerolog.ErrorFieldName = "error"
	zerolog.CallerFieldName = "caller"
	zerolog.TimeFieldFormat = "" // empty causes unix time to be logged as ts
	//zerolog.TimestampFunc = time.Now
	//zerolog.CallerSkipFrameCount = 2
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
}

func GetLogger() *zerolog.Logger {
	return &logger
}
