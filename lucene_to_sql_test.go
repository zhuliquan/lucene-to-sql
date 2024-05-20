package lucene_to_sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	esMapping "github.com/zhuliquan/es-mapping"
)

func TestLuceneToSQL(t *testing.T) {
	type testCase struct {
		name    string
		opts    []func(*SqlConvertor)
		query   string
		wantSQL string
		wantErr bool
	}

	for _, tt := range []testCase{
		{
			name: "test range left value error number",
			opts: []func(*SqlConvertor){
				WithSQLStyle(SQLite),
				WithSchema(&esMapping.PropertyMapping{}),
			},
			query: "",
			wantSQL: "",
		},
	} {
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
