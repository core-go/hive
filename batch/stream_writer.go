package batch

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
)

type StreamWriter struct {
	connection *hv.Connection
	tableName  string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
	schema     *h.Schema
	batchSize  int
	batch      []interface{}
}

func NewStreamWriter(connection *hv.Connection, tableName string, modelType reflect.Type, batchSize int, options ...func(context.Context, interface{}) (interface{}, error)) *StreamWriter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	schema := h.CreateSchema(modelType)
	return &StreamWriter{connection: connection, schema: schema, tableName: tableName, batchSize: batchSize, Map: mp}
}

func (w *StreamWriter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		w.batch = append(w.batch, m2)
	} else {
		w.batch = append(w.batch, model)
	}
	if len(w.batch) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}

func (w *StreamWriter) Flush(ctx context.Context) error {
	query, err := BuildToSaveBatch(w.tableName, w.batch, w.schema)
	if err != nil {
		return err
	}
	cursor := w.connection.Cursor()
	cursor.Exec(ctx, query)
	return cursor.Err
}
