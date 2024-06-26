package query

import (
	"context"
	hv "github.com/beltran/gohive"
	h "github.com/core-go/hive"
)

type Query[T any, K any, F any] struct {
	*Loader[T, K]
	BuildQuery func(F) string
}

func NewQuery[T any, K any, F any](connection *hv.Connection, tableName string, buildQuery func(F) string, options ...func(*T)) (*Query[T, K, F], error) {
	var mp func(*T)
	if len(options) >= 1 {
		mp = options[0]
	}
	loader, err := NewLoader[T, K](connection, tableName, mp)
	if err != nil {
		return nil, err
	}
	builder := &Query[T, K, F]{Loader: loader, BuildQuery: buildQuery}
	return builder, nil
}

func (b *Query[T, K, F]) Search(ctx context.Context, filter F, limit int64, offset int64) ([]T, int64, error) {
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
		l := len(res)
		for i := 0; i < l; i++ {
			b.Mp(&res[i])
		}
	}
	return res, count, err
}
