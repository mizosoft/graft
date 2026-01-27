package infra

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ParseConfigFile reads a cluster configuration file.
// Format: one line per node with "<node-id> <address>".
// Empty lines and lines starting with # are ignored.
func ParseConfigFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	addresses := make(map[string]string)
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments.
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("line %d: expected '<id> <address>', got: %s", lineNum, line)
		}

		addresses[fields[0]] = fields[1]
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return addresses, nil
}
