package lucene_to_sql

import (
	"fmt"
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

func (s *SQL) AddORClause(clause string, orSymbol bool) error {
	var err error
	if orSymbol {
		_, err = s.buff.WriteString(" OR ")
		if err != nil {
			return fmt.Errorf("failed to add OR operator, err: %+v", err)
		}
	}
	_, err = s.buff.WriteString(clause)
	if err != nil {
		return fmt.Errorf("failed to add OR clause, err: %+v", err)
	}
	return nil
}

func (s *SQL) AddAndClause(clause string, andSymbol, notSymbol bool) error {
	var err error
	if andSymbol {
		_, err = s.buff.WriteString(" AND ")
		if err != nil {
			return fmt.Errorf("failed to add AND operator, err: %+v", err)
		}
	}
	if notSymbol {
		_, err = s.buff.WriteString("NOT ")
		if err != nil {
			return fmt.Errorf("failed to add NOT operator, err: %+v", err)
		}
	}
	_, err = s.buff.WriteString(clause)
	if err != nil {
		return fmt.Errorf("failed to add AND clause, err: %+v", err)
	}
	return nil
}

func (s *SQL) AddSubClause(clause string, notSymbol bool) error {
	var err error
	if notSymbol {
		_, err = s.buff.WriteString(" NOT ")
		if err != nil {
			return fmt.Errorf("failed to add NOT operator, err: %+v", err)
		}
	}
	_, err = s.buff.WriteString(clause)
	if err != nil {
		return fmt.Errorf("failed to add sub clause, err: %+v", err)
	}
	return nil
}

func (s *SQL) String() string {
	return s.buff.String()
}
