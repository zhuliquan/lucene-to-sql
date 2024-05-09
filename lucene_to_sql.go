package lucene_to_sql

import (
	"fmt"
	// "strings"

	"github.com/zhuliquan/lucene_parser"
	// "github.com/zhuliquan/lucene_parser/term"
	esMapping "github.com/zhuliquan/es-mapping"
)

type Tokenizer interface {
	Split(string) []string
}

type SqlConvertor struct {
	// field => tokenizer
	tokenizers map[string]Tokenizer

	mappings *esMapping.Mapping
}

func WithTokenizer(field string, tokenizer Tokenizer) func(s *SqlConvertor) {
	return func(s *SqlConvertor) {
		s.tokenizers[field] = tokenizer
	}
}

func WithSchema(mappings *esMapping.Mapping) func(s *SqlConvertor) {
	return func(s *SqlConvertor) {
		s.mappings = mappings
	}
}

func NewSqlConvertor(options ...func(s *SqlConvertor)) *SqlConvertor {
	s := &SqlConvertor{tokenizers: make(map[string]Tokenizer)}
	for _, opt := range options {
		opt(s)
	}
	return s
}

func LuceneToSql(query string) (string, error) {
	lucene, err := lucene_parser.ParseLucene(query)
	if err != nil {
		return "", err
	}
	return luceneToSql(lucene)
}

func luceneToSql(lucene *lucene_parser.Lucene) (string, error) {
	sql := NewSQL()
	str, err := orQueryToSql(lucene.OrQuery)
	if err != nil {
		return "", fmt.Errorf("failed to convert OR clause, err: %+v", err)
	}
	err = sql.AddORClause(str, false)
	if err != nil {
		return "", err
	}
	for _, subQuery := range lucene.OSQuery {
		str, err = orQueryToSql(subQuery.OrQuery)
		if err != nil {
			return "", fmt.Errorf("failed to convert OR clause, err: %+v", err)
		}
		err = sql.AddORClause(str, true)
		if err != nil {
			return "", err
		}
	}
	return sql.String(), nil
}

func orQueryToSql(orQuery *lucene_parser.OrQuery) (string, error) {
	sql := NewSQL()
	str, err := andQueryToSql(orQuery.AndQuery)
	if err != nil {
		return "", fmt.Errorf("failed to convert AND clause, err: %+v", err)
	}
	err = sql.AddAndClause(str, false, false)
	if err != nil {
		return "", err
	}
	for _, subQuery := range orQuery.AnSQuery {
		reverse := false
		if subQuery.NotSymbol != nil {
			reverse = true
		}
		str, err = andQueryToSql(subQuery.AndQuery)
		if err != nil {
			return "", fmt.Errorf("failed to convert AND clause, err: %+v", err)
		}
		err = sql.AddAndClause(str, true, reverse)
		if err != nil {
			return "", err
		}
	}
	return sql.String(), nil
}

func andQueryToSql(andQuery *lucene_parser.AndQuery) (string, error) {
	reverse := false
	if andQuery.NotSymbol != nil {
		reverse = true
	}
	if andQuery.ParenQuery != nil {
		sql := NewSQL()
		str, err := parenToSql(andQuery.ParenQuery)
		if err != nil {
			return "", err
		}
		err = sql.AddSubClause(str, reverse)
		if err != nil {
			return "", fmt.Errorf("failed to convert Sub Lucene, err: %+v", err)
		}
		return sql.String(), nil
	} else {
		return fieldToSql(andQuery.FieldQuery, reverse)
	}
}

func parenToSql(parenQuery *lucene_parser.ParenQuery) (string, error) {
	str, err := luceneToSql(parenQuery.SubQuery)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("( %s )", str), nil
}

func fieldToSql(fieldQuery *lucene_parser.FieldQuery, reverse bool) (string, error) {
	// field := fieldQuery.Field.String()
	// value := fieldQuery.Term
	// termType := value.GetTermType()
	// if termType & term.SINGLE_TERM_TYPE == term.SINGLE_TERM_TYPE {
	// 	return "", nil
	// }
	// if termType & term.PHRASE_TERM_TYPE == term.PHRASE_TERM_TYPE {
	// 	return "", nil
	// }
	// if termType & term.REGEXP_TERM_TYPE == term.REGEXP_TERM_TYPE {
	// 	return "", nil
	// }
	// if termType & term.TermType(term.BOOLEAN_FIELD_TYPE) {
	// 	return "", nil
	// }

	// switch value.GetTermType() {

	// }
	return "", nil
}
