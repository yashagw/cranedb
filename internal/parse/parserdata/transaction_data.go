package parserdata

// BeginData represents a BEGIN or BEGIN TRANSACTION command
type BeginData struct{}

func NewBeginData() *BeginData {
	return &BeginData{}
}

// CommitData represents a COMMIT command
type CommitData struct{}

func NewCommitData() *CommitData {
	return &CommitData{}
}

// RollbackData represents a ROLLBACK command
type RollbackData struct{}

func NewRollbackData() *RollbackData {
	return &RollbackData{}
}
