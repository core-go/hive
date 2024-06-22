package query

import (
	"context"
	"errors"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
	"reflect"
)

type SearchBuilder[T any, F any] struct {
	Connection *hv.Connection
	BuildQuery func(F) string
	Mp         func(*T)
	Map        map[string]int
}

func NewSearchBuilder[T any, F any](connection *hv.Connection, buildQuery func(F) string, options ...func(*T)) (*SearchBuilder[T, F], error) {
	var mp func(*T)
	if len(options) >= 1 {
		mp = options[0]
	}
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		return nil, errors.New("T must be a struct")
	}
	fieldsIndex, err := h.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	builder := &SearchBuilder[T, F]{Connection: connection, Map: fieldsIndex, BuildQuery: buildQuery, Mp: mp}
	return builder, nil
}

func (b *SearchBuilder[T, F]) Search(ctx context.Context, m F, limit int64, offset int64) ([]T, int64, error) {
	sql := b.BuildQuery(m)
	query := h.BuildPagingQuery(sql, limit, offset)
	cursor := b.Connection.Cursor()
	defer cursor.Close()
	var res []T
	cursor.Exec(ctx, sql)
	if cursor.Err != nil {
		return res, -1, cursor.Err
	}
	err := h.Query(ctx, cursor, b.Map, &res, query)
	if err != nil {
		return res, -1, err
	}
	countQuery := h.BuildCountQuery(sql)
	cursor.Exec(ctx, countQuery)
	if cursor.Err != nil {
		return res, -1, cursor.Err
	}
	var count int64
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &count)
		if cursor.Err != nil {
			return res, count, cursor.Err
		}
	}
	if b.Mp != nil {
		l := len(res)
		for i := 0; i < l; i++ {
			b.Mp(&res[i])
		}
	}
	return res, count, err
}
