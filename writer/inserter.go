package writer

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
)

type Inserter[T any] struct {
	connection   *hv.Connection
	tableName    string
	Map          func(T)
	schema       *h.Schema
	VersionIndex int
}

func NewInserterWithMap[T any](connection *hv.Connection, tableName string, mp func(T), options ...int) *Inserter[T] {
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
	return &Inserter[T]{connection: connection, tableName: tableName, Map: mp, schema: schema, VersionIndex: versionIndex}
}

func NewInserter[T any](db *hv.Connection, tableName string, options ...func(T)) *Inserter[T] {
	var mp func(T)
	return NewInserterWithMap[T](db, tableName, mp)
}

func (w *Inserter[T]) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		w.Map(model)
	}
	stm := h.BuildToInsertWithVersion(w.tableName, model, w.VersionIndex, false, w.schema)
	cursor := w.connection.Cursor()
	cursor.Exec(ctx, stm)
	return cursor.Err
}
