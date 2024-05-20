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
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.INTEGER_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:[\"x\" TO *]",
			wantErr: true,
		},
		{
			name: "test range left value string value and include",
			opts: []func(*SqlConvertor){
				WithSQLStyle(SQLite),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:[87'yu TO *]",
			wantSQL: "field >= '87''yu'",
		},
		{
			name: "test range left phrase value string value and include",
			opts: []func(*SqlConvertor){
				WithSQLStyle(SQLite),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:[\"87'yu\" TO *]",
			wantSQL: "field >= '87''yu'",
		},
		{
			name: "test range left value number value and exclude",
			opts: []func(*SqlConvertor){
				WithSQLStyle(SQLite),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.INTEGER_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:{87 TO *]",
			wantSQL: "field > 87",
		},
		{
			name: "test range right value error number",
			opts: []func(*SqlConvertor){
				WithSQLStyle(SQLite),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.INTEGER_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:[87 TO \"x\"]",
			wantErr: true,
		},
		{
			name: "test range right value string value and include",
			opts: []func(*SqlConvertor){
				WithSQLStyle(SQLite),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:[* TO 87yu]",
			wantSQL: "field <= '87yu'",
		},
		{
			name: "test range right phrase value string value and include",
			opts: []func(*SqlConvertor){
				WithSQLStyle(SQLite),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:[* TO \"87yu\"]",
			wantSQL: "field <= '87yu'",
		},
		{
			name: "test range right value number value and exclude",
			opts: []func(*SqlConvertor){
				WithSQLStyle(SQLite),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.INTEGER_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:[* TO 87}",
			wantSQL: "field < 87",
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

func getSchema(mapping *esMapping.Mapping) *esMapping.PropertyMapping {
	res, _ := esMapping.NewPropertyMapping(mapping)
	return res
}
