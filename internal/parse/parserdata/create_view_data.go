package parserdata

type CreateViewData struct {
	viewName string
	query    *QueryData
}

func NewCreateViewData(viewName string, query *QueryData) *CreateViewData {
	return &CreateViewData{
		viewName: viewName,
		query:    query,
	}
}

func (c *CreateViewData) ViewName() string {
	return c.viewName
}

func (c *CreateViewData) Query() *QueryData {
	return c.query
}
