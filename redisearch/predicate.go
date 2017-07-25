package redisearch

type Operator string

const (
	Eq Operator = "="

	Gt  Operator = ">"
	Gte Operator = ">="

	Lt  Operator = "<"
	Lte Operator = "<="

	Between          Operator = "BETWEEN"
	BetweenInclusive Operator = "BETWEEEN_EXCLUSIVE"
)

type Predicate struct {
	Property string
	Operator Operator
	Value    []interface{}
}

func NewPredicate(property string, operator Operator, values ...interface{}) Predicate {
	return Predicate{
		Property: property,
		Operator: operator,
		Value:    values,
	}
}
func Equals(property string, value interface{}) Predicate {
	return NewPredicate(property, Eq, value)

}

func InRange(property string, min, max interface{}, inclusive bool) Predicate {
	operator := Between
	if inclusive {
		operator = BetweenInclusive
	}
	return NewPredicate(property, operator, min, max)

}

func LessThan(property string, value interface{}) Predicate {
	return NewPredicate(property, Lt, value)
}

func LessThanEquals(property string, value interface{}) Predicate {
	return NewPredicate(property, Lte, value)
}

func GreaterThan(property string, value interface{}) Predicate {
	return NewPredicate(property, Gt, value)
}

func GreaterThanEquals(property string, value interface{}) Predicate {
	return NewPredicate(property, Gte, value)
}
