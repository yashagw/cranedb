package metadata

import "github.com/yashagw/cranedb/internal/record"

// IndexInfo contains info necessary to estimate index costs and open the index
type IndexInfo struct {
	indexName   string
	fieldName   string
	tableSchema *record.Schema
	indexLayout *record.Layout
	stats       *StatInfo
}

// createIndexLayout builds the layout for index records: block, id, dataval
func createIndexLayout(tableLayout *record.Layout, fieldName string) *record.Layout {
	sch := record.NewSchema()
	sch.AddIntField("block")
	sch.AddIntField("id")

	if tableLayout.GetSchema().Type(fieldName) == "int" {
		sch.AddIntField("dataval")
	} else {
		fldLen := tableLayout.GetSchema().Length(fieldName)
		sch.AddStringField("dataval", fldLen)
	}

	return record.NewLayoutFromSchema(sch)
}
