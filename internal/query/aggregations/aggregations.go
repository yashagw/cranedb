package aggregations

import "github.com/yashagw/cranedb/internal/scan"

type AggregationFunction interface {
	// ProcessFirst initializes the aggregation with the first record in a group.
	// This is called once per group when GroupByScan encounters the first record
	// of a new group. The scan is positioned at that first record.
	// Implementations should extract the field value and initialize their state
	// (e.g., for MAX, set the first value as the initial maximum).
	ProcessFirst(s scan.Scan) error

	// ProcessNext updates the aggregation with the next record in the same group.
	// This is called for each subsequent record that belongs to the current group,
	// after ProcessFirst has been called. The scan is positioned at that record.
	// Implementations should extract the field value and update their state
	// (e.g., for MAX, compare and update if the new value is greater).
	ProcessNext(s scan.Scan) error

	// FieldName returns the name of the new aggregation field.
	FieldName() string

	// Value returns the computed aggregation value.
	Value() any
}
