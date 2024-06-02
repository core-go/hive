package writer

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
)

type Updater[T any] struct {
	connection   *hv.Connection
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	VersionIndex int
	schema       *h.Schema
}

func NewUpdater[T any](db *hv.Connection, tableName string, options ...func(context.Context, interface{}) (interface{}, error)) *Updater[T] {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewUpdaterWithVersion[T](db, tableName, mp)
}
func NewUpdaterWithVersion[T any](db *hv.Connection, tableName string, mp func(context.Context, interface{}) (interface{}, error), options ...int) *Updater[T] {
	version := -1
	if len(options) > 0 && options[0] >= 0 {
		version = options[0]
	}
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := h.CreateSchema(modelType)
	return &Updater[T]{connection: db, tableName: tableName, VersionIndex: version, schema: schema, Map: mp}
}

func (w *Updater[T]) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		stm := h.BuildToUpdateWithVersion(w.tableName, m2, w.VersionIndex, w.schema)
		cursor := w.connection.Cursor()
		cursor.Exec(ctx, stm)
		return cursor.Err
	}
	stm := h.BuildToUpdateWithVersion(w.tableName, model, w.VersionIndex, w.schema)
	cursor := w.connection.Cursor()
	cursor.Exec(ctx, stm)
	return cursor.Err
}
