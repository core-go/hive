package export

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
)

func NewExportAdapter[T any](connection *hv.Connection,
	buildQuery func(context.Context) string,
	transform func(context.Context, *T) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter[T], error) {
	return NewExporter(connection, buildQuery, transform, write, close)
}
func NewExportService[T any](connection *hv.Connection,
	buildQuery func(context.Context) string,
	transform func(context.Context, *T) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter[T], error) {
	return NewExporter(connection, buildQuery, transform, write, close)
}
func NewExporter[T any](connection *hv.Connection,
	buildQuery func(context.Context) string,
	transform func(context.Context, *T) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter[T], error) {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	fieldsIndex, err := h.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	return &Exporter[T]{Connection: connection, Write: write, Close: close, fieldsIndex: fieldsIndex, Transform: transform, BuildQuery: buildQuery}, nil
}

type Exporter[T any] struct {
	Connection  *hv.Connection
	fieldsIndex map[string]int
	Transform   func(context.Context, *T) string
	BuildQuery  func(context.Context) string
	Write       func(p []byte) (n int, err error)
	Close       func() error
}

func (s *Exporter[T]) Export(ctx context.Context) (int64, error) {
	query := s.BuildQuery(ctx)
	cursor := s.Connection.Cursor()
	defer s.Connection.Close()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return 0, cursor.Err
	}
	return s.ScanAndWrite(ctx, cursor)
}

func (s *Exporter[T]) ScanAndWrite(ctx context.Context, cursor *hv.Cursor) (int64, error) {
	defer cursor.Close()
	var i int64
	i = 0
	columns, mcols, er0 := h.GetColumns(cursor)
	if er0 != nil {
		return i, er0
	}
	for cursor.HasMore(ctx) {
		var obj T
		r, _ := h.StructScan(&obj, columns, s.fieldsIndex)
		fieldPointers := cursor.RowMap(ctx)
		if cursor.Err != nil {
			return i, cursor.Err
		}
		for _, c := range columns {
			if colm, ok := mcols[c]; ok {
				if v, ok := fieldPointers[colm]; ok {
					if v != nil {
						v = reflect.Indirect(reflect.ValueOf(v)).Interface()
						if fieldValue, ok := r[c]; ok && !h.IsZeroOfUnderlyingType(v) {
							err1 := h.ConvertAssign(fieldValue, v)
							if err1 != nil {
								return i, err1
							}
						}
					}
				}
			}
		}
		err1 := s.TransformAndWrite(ctx, s.Write, &obj)
		if err1 != nil {
			return i, err1
		}
		i = i + 1
	}
	return i, nil
}
func (s *Exporter[T]) TransformAndWrite(ctx context.Context, write func(p []byte) (n int, err error), model *T) error {
	line := s.Transform(ctx, model)
	_, er := write([]byte(line))
	return er
}
