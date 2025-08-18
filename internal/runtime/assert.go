package runtime

import "fmt"

func Assert(validExp bool, format string, args ...any) {
	if !validExp {
		panic("failed to assert at runtime: " + fmt.Sprintf(format, args...))
	}
}
