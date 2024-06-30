package adapter

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
	"strings"
)

type Writer[T any] struct {
	Connection *hv.Connection
	Table      string
	// Keys           []string
	Schema         *h.Schema
	JsonColumnMap  map[string]string
	versionIndex   int
	versionDBField string
}

func NewWriter[T any](connection *hv.Connection, tableName string) *Writer[T] {
	return NewWriterWithVersion[T](connection, tableName, "")
}
func NewWriterWithVersion[T any](connection *hv.Connection, tableName string, versionField string) *Writer[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	// _, idNames := h.FindPrimaryKeys(modelType)
	mapJsonColumnKeys := h.MapJsonColumn(modelType)
	schema := h.CreateSchema(modelType)

	adapter := &Writer[T]{Connection: connection, Table: tableName, Schema: schema, JsonColumnMap: mapJsonColumnKeys, versionIndex: -1}
	if len(versionField) > 0 {
		index := h.FindFieldIndex(modelType, versionField)
		if index >= 0 {
			_, dbFieldName, exist := h.GetFieldByIndex(modelType, index)
			if !exist {
				dbFieldName = strings.ToLower(versionField)
			}
			adapter.versionIndex = index
			adapter.versionDBField = dbFieldName
		}
	}
	return adapter
}

func (a *Writer[T]) Create(ctx context.Context, model T) (int64, error) {
	query := h.BuildToInsertWithVersion(a.Table, model, a.versionIndex, false, a.Schema)
	cursor := a.Connection.Cursor()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return -1, cursor.Err
	}
	return 1, nil
}
func (a *Writer[T]) Update(ctx context.Context, model T) (int64, error) {
	query := h.BuildToUpdateWithVersion(a.Table, model, a.versionIndex, a.Schema)
	cursor := a.Connection.Cursor()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return -1, cursor.Err
	}
	return 1, nil
}
func (a *Writer[T]) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	colMap := h.JSONToColumns(model, a.JsonColumnMap)
	query := h.BuildToPatchWithVersion(a.Table, colMap, a.Schema.SKeys, a.versionDBField)
	cursor := a.Connection.Cursor()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return -1, cursor.Err
	}
	return 1, nil
}
func (a *Writer[T]) Save(ctx context.Context, model T) (int64, error) {
	query := h.BuildToInsertWithVersion(a.Table, model, a.versionIndex, false, a.Schema)
	cursor := a.Connection.Cursor()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return -1, cursor.Err
	}
	return 1, nil
}
