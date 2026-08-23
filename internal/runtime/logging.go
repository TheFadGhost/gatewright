package runtime

import (
	"gatewright/internal/config"
	"gatewright/internal/obs"
)

// NewLoggerFromConfig builds the process logger from a loaded configuration:
// format, output destination and the access-log field subset come from
// observability.access_log; the colour decision is resolved once via
// obs.ColourPolicy from the --no-color flag and TTY detection. The cmd layer
// calls this after config.Load instead of hand-rolling obs.Options so the
// file cannot drift from the documented schema.
func NewLoggerFromConfig(cfg *config.Config, flagNoColor bool, isTTY bool) (obs.Logger, error) {
	al := cfg.Observability.AccessLog
	format := al.Format
	if format == "" {
		format = "json"
	}
	output := al.Output
	if output == "" {
		output = "stdout"
	}
	return obs.New(obs.Options{
		Format:  format,
		Output:  output,
		Fields:  al.Fields,
		NoColor: obs.ColourPolicy(flagNoColor, isTTY),
	})
}
