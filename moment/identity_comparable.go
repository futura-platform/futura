package moment

// Compile-time assertion that Identity is comparable
var _ = func() bool { var a, b Identity; return a == b }
