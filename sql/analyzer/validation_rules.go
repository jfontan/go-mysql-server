package analyzer

import (
	"errors"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"
)

var DefaultValidationRules = []ValidationRule{
	{"validate_resolved", validateIsResolved},
	{"validate_order_by", validateOrderBy},
}

func validateIsResolved(a *Analyzer, n sql.Node) error {
	if !n.Resolved() {
		return errors.New("plan is not resolved")
	}

	return nil
}

func validateOrderBy(a *Analyzer, n sql.Node) error {
	switch n := n.(type) {
	case *plan.Sort:
		for _, field := range n.SortFields {
			switch field.Column.(type) {
			case sql.AggregationExpression:
				return errors.New("OrderBy does not support aggregation expressions")
			}
		}
	}

	return nil
}
