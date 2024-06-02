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
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
	schema     *h.Schema
	batchSize  int
	batch      []T
}

func NewStreamWriter[T any](connection *hv.Connection, tableName string, batchSize int, options ...func(context.Context, interface{}) (interface{}, error)) *StreamWriter[T] {
	var mp func(context.Context, interface{}) (interface{}, error)
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
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		w.batch = append(w.batch, m2.(T))
	} else {
		w.batch = append(w.batch, model)
	}
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
	cursor := w.connection.Cursor()
	cursor.Exec(ctx, query)
	return cursor.Err
}
