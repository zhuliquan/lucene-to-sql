package lucene_to_sql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/vjeantet/jodaTime"
	"github.com/zhuliquan/datemath_parser"
	esMapping "github.com/zhuliquan/es-mapping"
	"github.com/zhuliquan/lucene_parser"
	"github.com/zhuliquan/lucene_parser/term"
)

type SQL_STYLE int32

const (
	Standard SQL_STYLE = iota // SQL99
	SQLite
	MySQL
	Oracle
	PostgreSQL
	ClickHouse
)

func (s SQL_STYLE) String() string {
	switch s {
	case SQLite:
		return "SQLite"
	case MySQL:
		return "MySQL"
	case Oracle:
		return "Oracle"
	case PostgreSQL:
		return "PostgreSQL"
	case ClickHouse:
		return "ClickHouse"
	default:
		return "SQL99"
	}
}

type Tokenizer interface {
	Split(string) []string
}

type SqlConvertor struct {
	// field => tokenizer
	tokenizers map[string]Tokenizer

	mappings *esMapping.PropertyMapping

	sqlStyle SQL_STYLE
}

func WithTokenizer(field string, tokenizer Tokenizer) func(s *SqlConvertor) {
	return func(s *SqlConvertor) {
		s.tokenizers[field] = tokenizer
	}
}

func WithSchema(mappings *esMapping.PropertyMapping) func(s *SqlConvertor) {
	return func(s *SqlConvertor) {
		s.mappings = mappings

	}
}

func WithSQLStyle(sqlStyle SQL_STYLE) func(s *SqlConvertor) {
	return func(s *SqlConvertor) {
		s.sqlStyle = sqlStyle
	}
}

func NewSqlConvertor(options ...func(s *SqlConvertor)) *SqlConvertor {
	s := &SqlConvertor{tokenizers: make(map[string]Tokenizer)}
	for _, opt := range options {
		opt(s)
	}
	return s
}

func (c *SqlConvertor) LuceneToSql(query string) (string, error) {
	lucene, err := lucene_parser.ParseLucene(query)
	if err != nil {
		return "", err
	}
	return c.luceneToSql(lucene)
}

func (c *SqlConvertor) luceneToSql(lucene *lucene_parser.Lucene) (string, error) {
	sql := NewSQL()
	str, err := c.orQueryToSql(lucene.OrQuery)
	if err != nil {
		return "", fmt.Errorf("failed to convert OR clause, err: %w", err)
	}
	err = sql.AddORClause(str, false)
	if err != nil {
		return "", err
	}
	for _, subQuery := range lucene.OSQuery {
		str, err = c.orQueryToSql(subQuery.OrQuery)
		if err != nil {
			return "", fmt.Errorf("failed to convert OR clause, err: %w", err)
		}
		err = sql.AddORClause(str, true)
		if err != nil {
			return "", err
		}
	}
	return sql.String(), nil
}

func (c *SqlConvertor) orQueryToSql(orQuery *lucene_parser.OrQuery) (string, error) {
	sql := NewSQL()
	str, err := c.andQueryToSql(orQuery.AndQuery)
	if err != nil {
		return "", fmt.Errorf("failed to convert AND clause, err: %w", err)
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
		str, err = c.andQueryToSql(subQuery.AndQuery)
		if err != nil {
			return "", fmt.Errorf("failed to convert AND clause, err: %w", err)
		}
		err = sql.AddAndClause(str, true, reverse)
		if err != nil {
			return "", err
		}
	}
	return sql.String(), nil
}

func (c *SqlConvertor) andQueryToSql(andQuery *lucene_parser.AndQuery) (string, error) {
	reverse := false
	if andQuery.NotSymbol != nil {
		reverse = true
	}
	if andQuery.ParenQuery != nil {
		sql := NewSQL()
		str, err := c.parenToSql(andQuery.ParenQuery)
		if err != nil {
			return "", err
		}
		err = sql.AddSubClause(str, reverse)
		if err != nil {
			return "", fmt.Errorf("failed to convert Sub Lucene, err: %w", err)
		}
		return sql.String(), nil
	} else {
		return c.termQueryToSql(andQuery.FieldQuery, reverse)
	}
}

func (c *SqlConvertor) parenToSql(parenQuery *lucene_parser.ParenQuery) (string, error) {
	str, err := c.luceneToSql(parenQuery.SubQuery)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("( %s )", str), nil
}

func (c *SqlConvertor) termQueryToSql(termQuery *lucene_parser.FieldQuery, reverse bool) (string, error) {
	field := termQuery.Field.String()
	if field == "" {
		return "", errors.New("field is empty")
	}
	value := termQuery.Term
	typMap, tErr := c.mappings.GetProperty(field)
	if tErr != nil || len(typMap) == 0 {
		return "", fmt.Errorf("failed to get field: %s property, err: %v", field, tErr)
	}
	tType := typMap[field]
	var sql string
	var err error
	switch {
	case value.GetTermType()&term.REGEXP_TERM_TYPE == term.REGEXP_TERM_TYPE:
		sql, err = c.regexpQueryToSql(field, tType, termQuery.Term)
	case value.GetTermType()&term.RANGE_TERM_TYPE == term.RANGE_TERM_TYPE:
		sql, err = c.rangeQueryToSql(field, tType, termQuery.Term)
	case value.GetTermType()&term.WILDCARD_TERM_TYPE == term.WILDCARD_TERM_TYPE:
		sql, err = c.wildcardQueryToSql(field, tType, termQuery.Term)
	case value.GetTermType()&term.FUZZY_TERM_TYPE == term.FUZZY_TERM_TYPE:
		sql, err = c.fuzzyQueryToSql(field, tType, termQuery.Term)
	case value.GetTermType()&term.SINGLE_TERM_TYPE == term.SINGLE_TERM_TYPE:
		sql, err = c.singleQueryToSql(field, tType, termQuery.Term)
	case value.GetTermType()&term.PHRASE_TERM_TYPE == term.PHRASE_TERM_TYPE:
		sql, err = c.phraseQueryToSql(field, tType, termQuery.Term)
	case value.GetTermType()&term.GROUP_TERM_TYPE == term.GROUP_TERM_TYPE:
		lucene := lucene_parser.TermGroupToLucene(termQuery.Field, value.TermGroup)
		sql, err = c.luceneToSql(lucene)
	}
	if err != nil {
		return "", err
	}
	if reverse {
		return fmt.Sprintf("NOT (%s)", sql), nil
	}
	return sql, nil
}

func (c *SqlConvertor) singleQueryToSql(
	field string, tType *esMapping.Property, value *term.Term,
) (string, error) {
	switch {
	case esMapping.CheckNumberType(tType.Type):
		return fmt.Sprintf("%s = %s", field, value.String()), nil
	case esMapping.CheckKeywordType(tType.Type) ||
		esMapping.CheckIPType(tType.Type) ||
		esMapping.CheckVersionType(tType.Type):
		val := strings.ReplaceAll(value.String(), "'", "''")
		return fmt.Sprintf("%s = %s%s%s", field, "%", val, "%"), nil
	case esMapping.CheckTextType(tType.Type):
		tokenizer, haveTk := c.tokenizers[field]
		if haveTk {
			sql := NewSQL()
			for _, term := range tokenizer.Split(value.String()) {
				val := strings.ReplaceAll(term, "'", "''")
				sql.AddORClause(fmt.Sprintf("%s like '%s%s%s'", field, "%", val, "%"), true)
			}
			return sql.String(), nil
		} else {
			val := strings.ReplaceAll(value.String(), "''", "'")
			return fmt.Sprintf("%s like '%s%s%s'", field, "%", val, "%"), nil
		}

	case esMapping.CheckDateType(tType.Type):

	}
	return "", nil
}

func (c *SqlConvertor) phraseQueryToSql(
	field string, _ *esMapping.Property, value *term.Term,
) (string, error) {
	val := strings.Trim(value.String(), "\"")
	val = strings.ReplaceAll(val, "'", "''")
	return fmt.Sprintf("%s like '%s%s%s'", field, "%", val, "%"), nil
}

func (c *SqlConvertor) rangeQueryToSql(
	field string, tType *esMapping.Property, value *term.Term,
) (string, error) {
	sql := NewSQL()
	bnd := value.GetBound()

	if lVal := bnd.LeftValue; !lVal.IsInf(0) {
		if esMapping.CheckNumberType(tType.Type) &&
			len(bnd.LeftValue.PhraseValue) != 0 {
			return "", fmt.Errorf("field: %s left bound expect number but got string", field)
		}
		var val, err = getSqlBound(lVal, tType)
		if err != nil {
			return "", err
		}
		if bnd.LeftInclude {
			sql.AddAndClause(fmt.Sprintf("%s >= %s", field, val), false, false)
		} else {
			sql.AddAndClause(fmt.Sprintf("%s > %s", field, val), false, false)
		}
	}

	if rVal := bnd.RightValue; !rVal.IsInf(0) {
		if esMapping.CheckNumberType(tType.Type) &&
			len(bnd.RightValue.PhraseValue) != 0 {
			return "", fmt.Errorf("field: %s right bound expect number but got string", field)
		}
		var val, err = getSqlBound(rVal, tType)
		if err != nil {
			return "", err
		}
		if bnd.RightInclude {
			sql.AddAndClause(fmt.Sprintf("%s <= %s", field, val), !bnd.LeftValue.IsInf(0), false)
		} else {
			sql.AddAndClause(fmt.Sprintf("%s < %s", field, val), !bnd.LeftValue.IsInf(0), false)
		}
	}
	return sql.String(), nil
}

const standardFormat = "yyyy-MM-dd HH:mm:ss"

func getSqlBound(rVal *term.RangeValue, tType *esMapping.Property) (string, error) {
	var val string
	if esMapping.CheckStringType(tType.Type) ||
		esMapping.CheckIPType(tType.Type) ||
		esMapping.CheckVersionType(tType.Type) {
		if len(rVal.SingleValue) != 0 {
			val = rVal.String()
		} else {
			val = strings.Trim(rVal.String(), "\"")
		}
		val = strings.ReplaceAll(val, "'", "''")
		val = fmt.Sprintf("'%s'", val)
	} else if esMapping.CheckDateType(tType.Type) {
		parser, _ := datemath_parser.NewDateMathParser(
			datemath_parser.WithFormat(strings.Split(tType.Format, "||")),
		)
		if len(rVal.SingleValue) != 0 {
			val = rVal.String()
		} else {
			val = strings.Trim(rVal.String(), "\"")
		}
		tt, err := parser.Parse(val)
		if err != nil {
			return "", err
		}
		return "'" + jodaTime.Format(standardFormat, tt) + "'", nil
	} else {
		val = rVal.String()
	}
	return val, nil
}

func (c *SqlConvertor) regexpQueryToSql(
	field string, tType *esMapping.Property, value *term.Term,
) (string, error) {
	if esMapping.CheckStringType(tType.Type) {
		val := strings.Trim(value.String(), "/")
		val = strings.ReplaceAll(val, "'", "''")
		switch c.sqlStyle {
		case SQLite, MySQL:
			return fmt.Sprintf("%s REGEXP '%s'", field, val), nil
		case Oracle:
			return fmt.Sprintf("regexp_like(%s, '%s')", field, val), nil
		case ClickHouse:
			return fmt.Sprintf("match(%s, '%s')", field, val), nil
		default:
			// sql99 and postgresql
			return fmt.Sprintf("%s SIMILAR TO '%s'", field, val), nil
		}
	} else {
		return "", fmt.Errorf("expect field: %s string type, but: %s", field, tType.Type)
	}
}

func (c *SqlConvertor) wildcardQueryToSql(
	field string, tType *esMapping.Property, value *term.Term,
) (string, error) {
	if esMapping.CheckStringType(tType.Type) {
		switch c.sqlStyle {
		case SQLite:
			val := strings.ReplaceAll(value.String(), "'", "''")
			return fmt.Sprintf("%s GLOB '%s'", field, val), nil
		default:
			tks := []string{value.FuzzyTerm.SingleTerm.Begin}
			tks = append(tks, value.FuzzyTerm.SingleTerm.Chars...)
			pattern := []string{}
			for _, tk := range tks {
				switch tk {
				case "?":
					pattern = append(pattern, "_")
				case "*":
					pattern = append(pattern, "%")
				default:
					pattern = append(pattern, tk)
				}
			}
			val := strings.Join(pattern, "")
			val = strings.ReplaceAll(val, "'", "''")
			return fmt.Sprintf("%s LIKE '%s'", field, val), nil
		}
	} else {
		return "", fmt.Errorf("expect field: %s string type, but: %s", field, tType.Type)
	}
}

func (c *SqlConvertor) fuzzyQueryToSql(
	field string, tType *esMapping.Property, value *term.Term,
) (string, error) {
	if value.FuzzyTerm.PhraseTerm != nil {
		return "", fmt.Errorf("don't support phrase fuzzy query")
	}
	// Levenshtein Distance
	fuzziness := int(value.FuzzyTerm.Fuzzy().Float())
	if fuzziness == -1 {
		fuzziness = 1
	}
	if esMapping.CheckStringType(tType.Type) {
		val := value.FuzzyTerm.SingleTerm.String()
		val = strings.ReplaceAll(val, "'", "''")
		switch c.sqlStyle {
		case PostgreSQL:
			// CREATE EXTENSION fuzzystrmatch;
			return fmt.Sprintf("levenshtein(%s, '%s') <= %d", field, val, fuzziness), nil
		case ClickHouse:
			return fmt.Sprintf("multiFuzzyMatchAny(%s, %d, '%s')", field, fuzziness, val), nil
		default:
			return "", fmt.Errorf("%s is not support fuzzy query", c.sqlStyle)
		}
	} else {
		return "", fmt.Errorf("expect field: %s string type, but: %s", field, tType.Type)
	}
}
