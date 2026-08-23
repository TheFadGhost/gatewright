// Package builtin registers every rate-limiting strategy shipped with
// Gatewright. Import it (directly or transitively) wherever limiters are
// instantiated or validated:
//
//	import _ "gatewright/internal/limiter/builtin"
package builtin

import (
	_ "gatewright/internal/limiter/concurrency"
	_ "gatewright/internal/limiter/fixedwindow"
	_ "gatewright/internal/limiter/leakybucket"
	_ "gatewright/internal/limiter/slidingcounter"
	_ "gatewright/internal/limiter/slidinglog"
	_ "gatewright/internal/limiter/tokenbucket"
)
