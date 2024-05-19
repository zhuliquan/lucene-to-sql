package lucene_to_sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLuceneToSQL(t *testing.T) {
	type testCase struct {
		name    string
		opts    []func(*SqlConvertor)
		query   string
		wantSQL string
		wantErr bool
	}

	for _, tt := range []testCase{} {
		t.Run(tt.name, func(t *testing.T) {
			cvt := NewSqlConvertor(tt.opts...)
			got, err := cvt.LuceneToSql(tt.query)
			if tt.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Equal(t, tt.wantSQL, got)
			}
		})
	}
}
