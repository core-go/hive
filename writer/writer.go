package writer

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
)

type Writer[T any] struct {
	connection   *hv.Connection
	tableName    string
	Map          func(T)
	schema       *h.Schema
	VersionIndex int
}

func NewWriterWithMap[T any](connection *hv.Connection, tableName string, mp func(T), options ...int) *Writer[T] {
	versionIndex := -1
	if len(options) > 0 && options[0] >= 0 {
		versionIndex = options[0]
	}
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := h.CreateSchema(modelType)
	return &Writer[T]{connection: connection, tableName: tableName, Map: mp, schema: schema, VersionIndex: versionIndex}
}

func NewWriter[T any](db *hv.Connection, tableName string, options ...func(T)) *Writer[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewWriterWithMap[T](db, tableName, mp)
}

func (w *Writer[T]) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		w.Map(model)
	}
	stm := h.BuildToInsertWithVersion(w.tableName, model, w.VersionIndex, true, w.schema)
	cursor := w.connection.Cursor()
	cursor.Exec(ctx, stm)
	return cursor.Err
}
