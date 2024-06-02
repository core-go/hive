package writer

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
)

type Writer struct {
	connection   *hv.Connection
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	schema       *h.Schema
	VersionIndex int
}

func NewWriterWithMap(connection *hv.Connection, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), options ...int) *Writer {
	versionIndex := -1
	if len(options) > 0 && options[0] >= 0 {
		versionIndex = options[0]
	}
	schema := h.CreateSchema(modelType)
	return &Writer{connection: connection, tableName: tableName, Map: mp, schema: schema, VersionIndex: versionIndex}
}

func NewWriter(db *hv.Connection, tableName string, modelType reflect.Type, options ...func(ctx context.Context, model interface{}) (interface{}, error)) *Writer {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewWriterWithMap(db, tableName, modelType, mp)
}

func (w *Writer) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		stm := h.BuildToInsertWithVersion(w.tableName, m2, w.VersionIndex, true, w.schema)
		cursor := w.connection.Cursor()
		cursor.Exec(ctx, stm)
		return cursor.Err
	}
	stm := h.BuildToInsertWithVersion(w.tableName, model, w.VersionIndex, true, w.schema)
	cursor := w.connection.Cursor()
	cursor.Exec(ctx, stm)
	return cursor.Err
}
