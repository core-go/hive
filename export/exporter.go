package export

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
)

func NewExportRepository(db *hv.Connection, modelType reflect.Type,
	buildQuery func(context.Context) string,
	transform func(context.Context, interface{}) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter, error) {
	return NewExporter(db, modelType, buildQuery, transform, write, close)
}
func NewExportAdapter(db *hv.Connection, modelType reflect.Type,
	buildQuery func(context.Context) string,
	transform func(context.Context, interface{}) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter, error) {
	return NewExporter(db, modelType, buildQuery, transform, write, close)
}
func NewExportService(db *hv.Connection, modelType reflect.Type,
	buildQuery func(context.Context) string,
	transform func(context.Context, interface{}) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter, error) {
	return NewExporter(db, modelType, buildQuery, transform, write, close)
}

func NewExporter(db *hv.Connection, modelType reflect.Type,
	buildQuery func(context.Context) string,
	transform func(context.Context, interface{}) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter, error) {
	fieldsIndex, err := h.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	return &Exporter{Connection: db, modelType: modelType, Write: write, Close: close, fieldsIndex: fieldsIndex, Transform: transform, BuildQuery: buildQuery}, nil
}

type Exporter struct {
	Connection  *hv.Connection
	modelType   reflect.Type
	fieldsIndex map[string]int
	Transform   func(context.Context, interface{}) string
	BuildQuery  func(context.Context) string
	Write       func(p []byte) (n int, err error)
	Close       func() error
}

func (s *Exporter) Export(ctx context.Context) error {
	query := s.BuildQuery(ctx)
	cursor := s.Connection.Cursor()
	defer s.Connection.Close()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return cursor.Err
	}
	return s.ScanAndWrite(ctx, cursor, s.modelType)
}

func (s *Exporter) ScanAndWrite(ctx context.Context, cursor *hv.Cursor, structType reflect.Type) error {
	defer cursor.Close()
	columns, mcols, er0 := h.GetColumns(cursor)
	if er0 != nil {
		return er0
	}
	for cursor.HasMore(ctx) {
		initModel := reflect.New(structType).Interface()
		r, _ := h.StructScan(initModel, columns, s.fieldsIndex)
		fieldPointers := cursor.RowMap(ctx)
		if cursor.Err != nil {
			return cursor.Err
		}
		for _, c := range columns {
			if colm, ok := mcols[c]; ok {
				if v, ok := fieldPointers[colm]; ok {
					if v != nil {
						v = reflect.Indirect(reflect.ValueOf(v)).Interface()
						if fieldValue, ok := r[c]; ok && !h.IsZeroOfUnderlyingType(v) {
							err1 := h.ConvertAssign(fieldValue, v)
							if err1 != nil {
								return err1
							}
						}
					}
				}
			}
		}
		err1 := s.TransformAndWrite(ctx, s.Write, initModel)
		if err1 != nil {
			return err1
		}
	}
	return nil
}
func (s *Exporter) TransformAndWrite(ctx context.Context, write func(p []byte) (n int, err error), model interface{}) error {
	line := s.Transform(ctx, model)
	_, er := write([]byte(line))
	return er
}
