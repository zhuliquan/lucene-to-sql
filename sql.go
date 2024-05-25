package lucene_to_sql

import (
	"strings"
)

type SQL struct {
	buff *strings.Builder
}

func NewSQL() *SQL {
	return &SQL{
		buff: &strings.Builder{},
	}
}

func (s *SQL) AddORClause(clause string, orSymbol bool) {
	if orSymbol {
		_, _ = s.buff.WriteString(" OR ")
	}
	_, _ = s.buff.WriteString(clause)
}

func (s *SQL) AddAndClause(clause string, andSymbol, notSymbol bool) {
	if andSymbol {
		_, _ = s.buff.WriteString(" AND ")
	}
	if notSymbol {
		_, _ = s.buff.WriteString("NOT ")
	}
	_, _ = s.buff.WriteString(clause)
}

func (s *SQL) AddSubClause(clause string, notSymbol bool) {
	if notSymbol {
		_, _ = s.buff.WriteString(" NOT ")
	}
	_, _ = s.buff.WriteString(clause)
}

func (s *SQL) String() string {
	return s.buff.String()
}
