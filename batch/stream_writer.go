package batch

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
)

type StreamWriter[T any] struct {
	connection *hv.Connection
	tableName  string
	Map        func(T)
	schema     *h.Schema
	batchSize  int
	batch      []T
}

func NewStreamWriter[T any](connection *hv.Connection, tableName string, batchSize int, options ...func(T)) *StreamWriter[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := h.CreateSchema(modelType)
	return &StreamWriter[T]{connection: connection, schema: schema, tableName: tableName, batchSize: batchSize, Map: mp}
}

func (w *StreamWriter[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	w.batch = append(w.batch, model)
	if len(w.batch) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}

func (w *StreamWriter[T]) Flush(ctx context.Context) error {
	query, err := BuildToSaveBatch[T](w.tableName, w.batch, w.schema)
	if err != nil {
		return err
	}
	defer func() {
		w.batch = make([]T, 0)
	}()
	cursor := w.connection.Cursor()
	cursor.Exec(ctx, query)
	return cursor.Err
}
