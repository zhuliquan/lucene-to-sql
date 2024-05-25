# lucene-to-sql

## Introduction

This package can parse lucene query and convert to **WHERE predicates** in SQL, this package is pure go package.

## Features

- 1、This package can convert lucene query to **WHERE predicates** SQL.
- 2、According to ES Mapping to convert Lucene query to SQL.

## Usage

get package:

```shell
go get github.com/zhuliquan/lucene-to-sql@latest
```

example of `lucene-to-sql`:

```golang
import (
    "fmt"

    "github.com/zhuliquan/lucene-to-sql"
    esMapping "github.com/zhuliquan/es-mapping"
)

func getSchema(mapping *esMapping.Mapping) *esMapping.PropertyMapping {
    res, _ := esMapping.NewPropertyMapping(mapping)
    return res
}

func main() {
    cvt := lucene_to_sql.NewSqlConvertor(
        lucene_to_sql.WithSQLStyle(lucene_to_sql.SQLite),
        lucene_to_sql.WithSchema(getSchema(&esMapping.Mapping{
            Properties: map[string]*esMapping.Property{
                "field1": {
                    Type:   esMapping.DATE_FIELD_TYPE,
                    Format: "yyyy-MM-dd'T'HH:mm:ss",
                },
                "field2": {
                    Type: esMapping.KEYWORD_FIELD_TYPE,
                },
                "field3": {
                    Type: esMapping.TEXT_FIELD_TYPE,
                },
            },
        })),
    )
    query := `field1:["2008-01-01T09:09:08" TO * ] AND field2:foo OR field3:bar`
    got, err := cvt.LuceneToSql(query)
    if err != nil {
        panic(err)
    } else {
        // field1 >= '2008-01-01 09:09:08' AND field2 = 'foo' OR field3 like '%bar%'
        fmt.Println(got)
    }
}
```
