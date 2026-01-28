package infra

import (
	"fmt"
	"strings"
)

// ParseAddressList parses an inline address list with the format "id=addr,id=addr,...".
func ParseAddressList(s string) (map[string]string, error) {
	addresses := make(map[string]string)
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid entry %q: expected 'id=address'", entry)
		}

		addresses[parts[0]] = parts[1]
	}
	return addresses, nil
}
