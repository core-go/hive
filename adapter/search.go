package adapter

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
)

type SearchAdapter[T any, K any, F any] struct {
	*GenericAdapter[T, K]
	BuildQuery func(F) string
	Mp         func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewSearchAdapterWithVersion[T any, K any, F any](connection *hv.Connection, tableName string, buildQuery func(F) string, versionField string, options ...func(context.Context, interface{}) (interface{}, error)) (*SearchAdapter[T, K, F], error) {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	adapter, err := NewGenericAdapterWithVersion[T, K](connection, tableName, versionField)
	if err != nil {
		return nil, err
	}
	builder := &SearchAdapter[T, K, F]{GenericAdapter: adapter, BuildQuery: buildQuery, Mp: mp}
	return builder, nil
}

func (b *SearchAdapter[T, K, F]) Search(ctx context.Context, filter F, limit int64, offset int64) ([]T, int64, error) {
	sql := b.BuildQuery(filter)
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
		_, err := h.MapModels(ctx, &res, b.Mp)
		return res, count, err
	}
	return res, count, err
}
