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
	Map          func(T)
	VersionIndex int
	schema       *h.Schema
}

func NewUpdater[T any](db *hv.Connection, tableName string, options ...func(T)) *Updater[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewUpdaterWithVersion[T](db, tableName, mp)
}
func NewUpdaterWithVersion[T any](db *hv.Connection, tableName string, mp func(T), options ...int) *Updater[T] {
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
		w.Map(model)
	}
	stm := h.BuildToUpdateWithVersion(w.tableName, model, w.VersionIndex, w.schema)
	cursor := w.connection.Cursor()
	cursor.Exec(ctx, stm)
	return cursor.Err
}
