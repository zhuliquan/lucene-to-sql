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
		{
			name: "test regexp error",
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
			query:   "field:/xx/",
			wantErr: true,
		},
		{
			name: "test regexp oracle",
			opts: []func(*SqlConvertor){
				WithSQLStyle(Oracle),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:/x'x+/",
			wantSQL: "regexp_like(field, 'x''x+')",
		},
		{
			name: "test regexp ClickHouse",
			opts: []func(*SqlConvertor){
				WithSQLStyle(ClickHouse),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:/x'x+/",
			wantSQL: "match(field, 'x''x+')",
		},
		{
			name: "test regexp sqlite,mysql",
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
			query:   "field:/x'x+/",
			wantSQL: "field REGEXP 'x''x+'",
		},
		{
			name: "test regexp postgresql",
			opts: []func(*SqlConvertor){
				WithSQLStyle(PostgreSQL),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:/x'x+/",
			wantSQL: "field SIMILAR TO 'x''x+'",
		},
		{
			name: "test wildcard error",
			opts: []func(*SqlConvertor){
				WithSQLStyle(PostgreSQL),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.INTEGER_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:x'*",
			wantErr: true,
		},
		{
			name: "test wildcard sqlite",
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
			query:   "field:x'*",
			wantSQL: "field GLOB 'x''*'",
		},
		{
			name: "test wildcard other sql",
			opts: []func(*SqlConvertor){
				WithSQLStyle(PostgreSQL),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:x'x?x*",
			wantSQL: "field LIKE 'x''x_x%'",
		},
		{
			name: "test phrase fuzzy error",
			opts: []func(*SqlConvertor){
				WithSQLStyle(PostgreSQL),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:\"xx yy\"~",
			wantErr: true,
		},
		{
			name: "test text fuzzy error",
			opts: []func(*SqlConvertor){
				WithSQLStyle(PostgreSQL),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.INTEGER_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:you~",
			wantErr: true,
		},
		{
			name: "test fuzzy postgresql",
			opts: []func(*SqlConvertor){
				WithSQLStyle(PostgreSQL),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:you'~",
			wantSQL: "levenshtein(field, 'you''') <= 1",
		},
		{
			name: "test fuzzy click_house",
			opts: []func(*SqlConvertor){
				WithSQLStyle(ClickHouse),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:you'~2",
			wantSQL: "multiFuzzyMatchAny(field, 2, 'you''')",
		},
		{
			name: "test fuzzy other sql",
			opts: []func(*SqlConvertor){
				WithSQLStyle(MySQL),
				WithSchema(getSchema(&esMapping.Mapping{
					Properties: map[string]*esMapping.Property{
						"field": {
							Type: esMapping.TEXT_FIELD_TYPE,
						},
					},
				})),
			},
			query:   "field:you'~2",
			wantErr: true,
		},
		{
			name: "test left include and right exclude",
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
			query:   "field:[1 TO 2}",
			wantSQL: "field >= 1 AND field < 2",
		},
		{
			name: "test left include and right include",
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
			query:   "field:[1 TO 2]",
			wantSQL: "field >= 1 AND field <= 2",
		},
		{
			name: "test left exclude and right exclude",
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
			query:   "field:{0 TO 2}",
			wantSQL: "field > 0 AND field < 2",
		},
		{
			name: "test left exclude and right include",
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
			query:   "field:{0 TO 2]",
			wantSQL: "field > 0 AND field <= 2",
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
