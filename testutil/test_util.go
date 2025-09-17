package testutil

import (
	"testing"

	"gotest.tools/v3/assert"
)

func NilOf[T any]() T {
	var zero T
	return zero
}

func AssertNil[T any](t *testing.T, value T) {
	assert.Equal(t, value, NilOf[T]())
}
