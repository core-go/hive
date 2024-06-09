package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
)

type Loader[T any, K any] struct {
	Connection    *hv.Connection
	Mp            func(ctx context.Context, model interface{}) (interface{}, error)
	keys          []string
	JsonColumnMap map[string]string
	Map           map[string]int
	Table         string
	IdMap         bool
}

func NewLoader[T any, K any](connection *hv.Connection, tableName string, options ...func(context.Context, interface{}) (interface{}, error)) (*Loader[T, K], error) {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	_, primaryKeys := h.FindPrimaryKeys(modelType)
	var k K
	kType := reflect.TypeOf(k)
	idMap := false
	if len(primaryKeys) > 1 {
		if kType.Kind() == reflect.Map {
			idMap = true
		} else if kType.Kind() != reflect.Struct {
			return nil, errors.New("for composite keys, K must be a struct or a map")
		}
	}

	mapJsonColumnKeys := h.MapJsonColumn(modelType)

	fieldsIndex, er0 := h.GetColumnIndexes(modelType)
	if er0 != nil {
		return nil, er0
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &Loader[T, K]{Connection: connection, Mp: mp, keys: primaryKeys, JsonColumnMap: mapJsonColumnKeys, Map: fieldsIndex, Table: tableName, IdMap: idMap}, nil
}

func (s *Loader[T, K]) All(ctx context.Context) ([]T, error) {
	query := h.BuildSelectAllQuery(s.Table)
	cursor := s.Connection.Cursor()
	defer cursor.Close()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return nil, cursor.Err
	}
	var res []T
	err := h.Query(ctx, cursor, s.Map, &res, query)
	if err == nil {
		if s.Mp != nil {
			h.MapModels(ctx, &res, s.Mp)
		}
	}
	return res, err
}
func toMap(obj interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	im := make(map[string]interface{})
	er2 := json.Unmarshal(b, &im)
	return im, er2
}
func (s *Loader[T, K]) getId(k K) (interface{}, error) {
	if len(s.keys) >= 2 && !s.IdMap {
		ri, err := toMap(k)
		return ri, err
	} else {
		return k, nil
	}
}
func (s *Loader[T, K]) Load(ctx context.Context, id K) (*T, error) {
	ip, er0 := s.getId(id)
	if er0 != nil {
		return nil, er0
	}
	queryAll := fmt.Sprintf("select * from %s ", s.Table)
	query := h.BuildFindById(queryAll, ip, s.JsonColumnMap, s.keys)
	cursor := s.Connection.Cursor()
	defer cursor.Close()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return nil, cursor.Err
	}
	var res []T
	err := h.Query(ctx, cursor, s.Map, &res, query)
	if err != nil {
		return nil, err
	}
	if len(res) > 0 {
		return &res[0], nil
	}
	return nil, nil
}
func (s *Loader[T, K]) Exist(ctx context.Context, id K) (bool, error) {
	ip, er0 := s.getId(id)
	if er0 != nil {
		return false, er0
	}
	queryAll := fmt.Sprintf("select * from %s ", s.Table)
	query := h.BuildFindById(queryAll, ip, s.JsonColumnMap, s.keys)
	cursor := s.Connection.Cursor()
	defer cursor.Close()
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return false, cursor.Err
	}
	for cursor.HasMore(ctx) {
		return true, nil
	}
	return false, nil
}
