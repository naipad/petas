// Package guixu implements PETAS v0.2 storage core.
//
// v0.2 goals:
//  1. Key/value separation with stable value pointers.
//  2. Versioned manifest/edit-log recovery protocol.
//  3. Adaptive GC scheduler with rate limiting and observability.
//
// This package is designed to be wired under pkg/petas while preserving
// business-layer APIs in pkg/petas.
package guixu
