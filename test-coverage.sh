go test -coverprofile=coverage.txt ./...

go tool cover -html=coverage.txt -o coverage.html

# Open coverage report if not in CI
if [ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ]; then
  open coverage.html
fi