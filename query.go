package litex

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cast"
)

// isIdentifier reports whether value is a simple or dotted SQL identifier ("id", "users.id"). This is
// what keeps interpolated ordering and grouping fields from becoming an injection vector.
func isIdentifier(value string) bool {
	// True where the next byte begins a segment, which rejects empty and digit-led segments.
	segmentStart := true

	for i := range len(value) {
		switch c := value[i]; {
		case c == '.':
			if segmentStart {
				return false
			}

			segmentStart = true
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c == '_':
			segmentStart = false
		case c >= '0' && c <= '9':
			if segmentStart {
				return false
			}
		default:
			return false
		}
	}

	return !segmentStart
}

// Operators for Where that the query builders treat specially; every other operator passes through
// verbatim. Matching is per comparison, so one clause can mix both, and neither follows
// Filter.ExactMatchByDefault, which resolves only a plain "=".
const (
	// OpExact matches the column against the whole value, normalized to SQL "=". Use it for IDs,
	// enums, booleans, and other exact-valued columns.
	OpExact = "=="

	// OpSubstring matches a value anywhere in the column, rewritten to "LIKE ?" with the bound value
	// wrapped in "%" wildcards. On numeric columns this silently over-matches (12 matches 1, 2, 123,
	// ...), so keep it to free text.
	OpSubstring = "=~"
)

// likeClause is what a rewritten substring comparison emits; the ESCAPE is what activates the
// escaping wrapArg applies to the bound value.
const likeClause = ` LIKE ? ESCAPE '\'`

// likeEscaper neutralizes LIKE metacharacters in a bound value. Replacer makes a single left-to-right
// pass, so the backslashes it inserts are not themselves re-escaped.
var likeEscaper = strings.NewReplacer(`\`, `\\`, "%", `\%`, "_", `\_`)

// Filter supplies optional limit, ordering, keyset pagination, and grouping controls to the query
// builders. Every field is optional: the zero value, or a nil *Filter, returns all matching rows
// ordered by the primary field descending, with no LIMIT.
//
// Every field is tagged json:"-", so the struct can be embedded in request types without appearing in
// their JSON.
type Filter struct {
	// Keyset cursor: excludes rows ordered past this value on the OrderBy column. That column should be
	// unique, otherwise ties at a page boundary may be skipped.
	Offset any `json:"-"`

	GroupBy string `json:"-"` // Comma-separated columns to GROUP BY.
	OrderBy string `json:"-"` // Column to order by; defaults to the primary field.
	Limit   int    `json:"-"` // Max rows to return; <= 0 omits the LIMIT clause (all rows).

	// ExactMatchByDefault decides only what a plain "=" means; clauses pinned with OpSubstring or
	// OpExact keep their behaviour either way.
	ExactMatchByDefault bool `json:"-"`

	OrderAsc bool `json:"-"` // Order ascending; defaults to descending.
}

// buildQueryCore contains the shared logic for BuildQuery and BuildComplexQuery.
func buildQueryCore(table string, primaryField string, fields string, filter *Filter, joins, groupBy []string, fn func(where []string, args []any) ([]string, []any)) (string, []any, error) {
	if filter == nil {
		filter = &Filter{}
	}

	if err := validateIdentifier("primary field", primaryField); err != nil {
		return "", nil, err
	}

	var args []any
	var where []string

	where, args = fn(where, args)

	if err := rewriteOperators(where, args, filter.ExactMatchByDefault); err != nil {
		return "", nil, err
	}

	orderBy := primaryField
	orderDirection := "DESC"

	if filter.OrderAsc {
		orderDirection = "ASC"
	}

	if filter.OrderBy != "" {
		if err := validateIdentifier("order by", filter.OrderBy); err != nil {
			return "", nil, err
		}

		orderBy = filter.OrderBy
	}

	// Keyset pagination must seek on the same column the rows are ordered by; seeking on any other
	// silently skips or duplicates rows across pages.
	if filter.Offset != nil {
		operator := "<"
		if filter.OrderAsc {
			operator = ">"
		}

		where, args = append(where, Where(orderBy, operator)), append(args, filter.Offset)
	}

	if len(where) == 0 {
		where = append(where, "1 = 1")
	}

	joinClause := ""
	if len(joins) > 0 {
		joinClause = " " + strings.Join(joins, " ")
	}

	groupByClause := ""
	if filter.GroupBy != "" {
		var err error
		if fields, groupBy, err = applyGroupBy(fields, filter.GroupBy, groupBy); err != nil {
			return "", nil, err
		}
	}

	if len(groupBy) > 0 {
		groupByClause = " GROUP BY " + strings.Join(groupBy, ", ")
	}

	limitClause := ""
	if filter.Limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", filter.Limit)
	}

	return fmt.Sprintf("SELECT %s FROM %s%s WHERE %s%s ORDER BY %s %s%s",
		fields, table, joinClause, strings.Join(where, " AND "), groupByClause, orderBy, orderDirection, limitClause), args, nil
}

// applyGroupBy merges Filter.GroupBy into the builder's own grouping columns and aggregates the select
// fields to match. Simple non-grouped columns are wrapped in max() so the aggregation is valid
// ("t.name AS n" becomes "max(t.name) AS n"); SQLite would otherwise return an arbitrary row's value.
// Fields that are already expressions pass through untouched.
func applyGroupBy(fields, filterGroupBy string, groupBy []string) (string, []string, error) {
	if strings.TrimSpace(fields) == "*" {
		return "", nil, errors.New("group by requires explicit select fields; '*' is not supported")
	}

	for f := range strings.SplitSeq(filterGroupBy, ",") {
		trimmed := strings.TrimSpace(f)
		if err := validateIdentifier("group by", trimmed); err != nil {
			return "", nil, err
		}

		groupBy = append(groupBy, trimmed)
	}

	grouped := make(map[string]bool, len(groupBy))
	for _, f := range groupBy {
		grouped[strings.TrimSpace(f)] = true
	}

	fieldList := splitTopLevel(fields)
	for i, f := range fieldList {
		trimmed := strings.TrimSpace(f)

		column, alias := splitAlias(trimmed)
		if isIdentifier(column) && !grouped[column] && !grouped[trimmed] {
			fieldList[i] = fmt.Sprintf("max(%s)%s", column, alias)
		} else {
			fieldList[i] = trimmed
		}
	}

	return strings.Join(fieldList, ", "), groupBy, nil
}

// rewriteOperators resolves OpExact, OpSubstring and plain "=" clauses in place, wrapping the bound
// value in "%" wildcards for each clause it turns into a LIKE. See the operator constants for the
// rules; exactByDefault carries Filter.ExactMatchByDefault.
func rewriteOperators(where []string, args []any, exactByDefault bool) error {
	argPos := 0

	for i, clause := range where {
		rewritten, next, err := rewriteClause(clause, args, argPos, exactByDefault)
		if err != nil {
			return err
		}

		where[i], argPos = rewritten, next
	}

	return nil
}

// rewriteClause resolves every special operator in one clause, returning the rewritten clause and the
// argument index the next clause starts at. argPos indexes this clause's first placeholder, and
// placeholders are counted left to right so each rewrite wraps the value bound to that operator. Only
// an operator directly followed by a placeholder counts: in "deleted = 0 AND name = ?" the first "="
// binds nothing. Single-quoted literals are copied through untouched.
func rewriteClause(clause string, args []any, argPos int, exactByDefault bool) (string, int, error) {
	var b strings.Builder
	var quoted bool
	var seen int // Placeholders passed so far, so argPos+seen indexes the next one.

	exactOp, subOp := " "+OpExact+" ", " "+OpSubstring+" "

	for i := 0; i < len(clause); {
		switch {
		case clause[i] == '\'':
			quoted = !quoted

			b.WriteByte(clause[i])
			i++
		case quoted:
			b.WriteByte(clause[i])
			i++
		case clause[i] == '?':
			seen++

			b.WriteByte(clause[i])
			i++
		case strings.HasPrefix(clause[i:], exactOp):
			b.WriteString(" = ")
			i += len(exactOp)
		case strings.HasPrefix(clause[i:], subOp+"?"):
			// The placeholder is consumed here rather than by the '?' case, so ESCAPE lands after it.
			b.WriteString(likeClause)
			i += len(subOp) + 1

			if err := wrapArg(args, argPos+seen, clause); err != nil {
				return "", 0, err
			}

			seen++
		case !exactByDefault && strings.HasPrefix(clause[i:], " = ?"):
			b.WriteString(likeClause)
			i += len(" = ?")

			if err := wrapArg(args, argPos+seen, clause); err != nil {
				return "", 0, err
			}

			seen++
		default:
			b.WriteByte(clause[i])
			i++
		}
	}

	return b.String(), argPos + seen, nil
}

// wrapArg surrounds the argument at i with "%" wildcards so its rewritten LIKE matches anywhere in the
// column. An out-of-range index is ignored: a clause may name more placeholders than the caller bound.
// A value with no string form is rejected rather than wrapped, since substituting "" would build
// LIKE '%%' and return the whole table; bind uuid.UUID and structs with OpExact.
// Any "%" or "_" in the value is escaped so it matches literally; a bare "%" would otherwise match
// every row.
func wrapArg(args []any, i int, clause string) error {
	if i >= len(args) {
		return nil
	}

	if args[i] == nil {
		return fmt.Errorf("substring match on a nil value in clause %q: use %s for an exact comparison", clause, OpExact)
	}

	value, err := cast.ToStringE(args[i])
	if err != nil {
		return fmt.Errorf("substring match on a %T in clause %q: %w: use %s, or convert the value to a string", args[i], clause, err, OpExact)
	}

	args[i] = "%" + likeEscaper.Replace(value) + "%"

	return nil
}

// splitAlias separates a select field from a trailing "AS <name>", returning the bare column and the
// alias suffix to re-append (empty when there is none). The suffix keeps its original spelling, so an
// aggregate can be wrapped around the column without disturbing the name it is returned under.
func splitAlias(field string) (string, string) {
	// Scanned case-insensitively on the original: an index found in an upper-cased copy would not line
	// up, since a few runes change byte length when cased.
	const sep = " AS "

	for i := len(field) - len(sep); i >= 0; i-- {
		if strings.EqualFold(field[i:i+len(sep)], sep) {
			return strings.TrimSpace(field[:i]), field[i:]
		}
	}

	return field, ""
}

// validateIdentifier rejects a table or column identifier that would be unsafe to interpolate into
// SQL. It permits simple and dotted identifiers only.
func validateIdentifier(kind string, value string) error {
	if !isIdentifier(value) {
		return fmt.Errorf("invalid %s %q: must be a simple or dotted identifier", kind, value)
	}

	return nil
}

// splitTopLevel splits a comma-separated field list on top-level commas only, leaving commas inside
// parentheses (function-call arguments) or single-quoted string literals intact.
func splitTopLevel(fields string) []string {
	var depth int
	var parts []string
	var quoted bool
	var start int

	for i, r := range fields {
		switch {
		case r == '\'':
			quoted = !quoted
		case quoted:
			// Inside a string literal: parentheses and commas are data, not structure.
		case r == '(':
			depth++
		case r == ')':
			if depth > 0 {
				depth--
			}
		case r == ',' && depth == 0:
			parts = append(parts, fields[start:i])
			start = i + 1
		}
	}

	return append(parts, fields[start:])
}

// BuildQuery creates a SELECT statement with filtering and ordering, the fn callback populating the
// WHERE clauses and their arguments. Pass nil for filter to return all rows ordered by primaryField
// DESC. See OpSubstring and OpExact for how clause operators are rewritten.
//
// Identifiers are interpolated rather than bound, so table, fields and joins must be trusted values.
// primaryField, Filter.OrderBy and Filter.GroupBy are validated as simple or dotted identifiers.
func BuildQuery(table string, primaryField string, fields string, filter *Filter, fn func(where []string, args []any) ([]string, []any)) (string, []any, error) {
	return buildQueryCore(table, primaryField, fields, filter, nil, nil, fn)
}

// BuildComplexQuery is BuildQuery with JOINs and GROUP BY, for joins or aggregation such as
// GROUP_CONCAT over a one-to-many. Its operator and identifier rules are the same, except that the
// groupBy argument is not validated, so it may hold expressions but must be a trusted value.
//
// Example:
//
//	BuildComplexQuery(
//	    "participants", "participants.id", "participants.*, GROUP_CONCAT(ases.id) AS asns",
//	    filter,
//	    []string{"LEFT JOIN ases ON ases.claimee_id = participants.id"},
//	    []string{"participants.id"},
//	    func(where []string, args []any) ([]string, []any) {
//	        if filter.ID != "" {
//	            where = append(where, litex.Where("participants.id", litex.OpExact))
//	            args = append(args, filter.ID)
//	        }
//	        return where, args
//	    },
//	)
func BuildComplexQuery(table string, primaryField string, fields string, filter *Filter, joins []string, groupBy []string,
	fn func(where []string, args []any) ([]string, []any),
) (string, []any, error) {
	return buildQueryCore(table, primaryField, fields, filter, joins, groupBy, fn)
}

// UpdateQuery creates an UPDATE statement of the form "UPDATE table SET a = ?, b = ?", the fn callback
// populating the fields and their arguments. It deliberately emits no WHERE clause: the caller must
// append one, otherwise the statement updates every row in the table.
//
// Field names are interpolated, so each is validated as a simple or dotted identifier. table is not,
// and must be a trusted value.
func UpdateQuery(table string, fn func(fields []string, args []any) ([]string, []any)) (string, []any, error) {
	var args []any
	var fields []string

	fields, args = fn(fields, args)

	if len(fields) == 0 {
		return "", nil, errors.New("missing fields")
	}

	if len(fields) != len(args) {
		return "", nil, fmt.Errorf("unequal fields:%v and args:%v", len(fields), len(args))
	}

	for _, field := range fields {
		if err := validateIdentifier("update field", field); err != nil {
			return "", nil, err
		}
	}

	return fmt.Sprintf("UPDATE %s SET %s = ?",
		table, strings.Join(fields, " = ?, ")), args, nil
}

// Where builds a "field <operator> ?" clause fragment. The operator is optional; omitting it emits a
// plain "=", read as substring matching unless Filter.ExactMatchByDefault is set. Pass OpSubstring or
// OpExact to pin the clause regardless of that setting, or any other SQL operator to pass it through
// verbatim.
func Where(field string, operator ...string) string {
	if len(operator) > 0 {
		return field + " " + operator[0] + " ?"
	}

	return field + " = ?"
}
