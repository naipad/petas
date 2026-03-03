package petas

import "errors"

var (
	ErrInvalidBucket = errors.New("petas: invalid bucket")
	ErrEmptyKey      = errors.New("petas: empty key")
	ErrInvalidToken  = errors.New("petas: token contains invalid delimiter")
	ErrBatchClosed   = errors.New("petas: batch closed")
)
