package futura

import "fmt"

// When runs fn only while cond is true, and tracks the branch's lifecycle across replays.
// Whenever cond transtitions from false to true, the steps inside fn are given fresh identities,
// so that they execute fresh rather than using memoized results.
func When(b FlowBuilder, cond bool, fn func(b FlowBuilder) error) error {
	open := State(b, false)
	incarnation := State(b, 0)
	if cond != open.V() {
		open.Set(cond)
		if cond {
			incarnation.Set(incarnation.V() + 1)
		}
	}
	if !cond {
		return nil
	}
	return fn(b.WithKey(fmt.Sprintf("when[%d]", incarnation.V())))
}
