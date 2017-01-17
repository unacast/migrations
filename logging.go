package migrations

import "log"

// LogError does error logging if set. It's silent if not.
var LogError *log.Logger

// LogDebug does debug logging if set. It's silent if not.
var LogDebug *log.Logger

func logError(v ...interface{}) {
	if LogError == nil {
		return
	}
	LogError.Println(v)
}

func logDebug(v ...interface{}) {
	if LogDebug == nil {
		return
	}
	LogDebug.Println(v)
}
