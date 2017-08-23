package redisearch

import (
	"fmt"
)

// MultiError Represents one or more errors
type MultiError []error

// NewMultiError initializes a multierror with the given len, and all sub-errors set to nil
func NewMultiError(len int) MultiError {
	return make(MultiError, len)
}

// Error returns a string representation of the error, in this case it just chains all the sub errors if they are not nil
func (e MultiError) Error() string {

	var ret string
	for i, err := range e {
		if err != nil {
			ret += fmt.Sprintf("[%d] %s\n", i, err.Error())
		}
	}
	return ret
}
