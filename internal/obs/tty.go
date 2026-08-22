package obs

import (
	"os"
)

// isWriterTTY reports whether w is an interactive terminal. On Windows it
// checks the console file modes; on POSIX, character devices.
func isWriterTTY(w any) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
