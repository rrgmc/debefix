package value

import (
	"fmt"
	"time"
)

// ValueBaseTime is a [debefix.ValueCalculator] calculate times based on a base time and a duration.
// In addition to [time.Duration], 3 more suffixes are supported: d (days), w (weeks), y (years).
type ValueBaseTime struct {
	baseTime time.Time
	typeName string
}

func NewValueBaseTime(baseTime time.Time, options ...ValueTimeOption) *ValueBaseTime {
	ret := &ValueBaseTime{
		baseTime: baseTime,
		typeName: "basetime",
	}
	for _, opt := range options {
		opt(ret)
	}
	return ret
}

func (v ValueBaseTime) CalculateValue(typ string, parameter string) (bool, any, error) {
	if typ != v.typeName {
		return false, nil, nil
	}

	if parameter == "" {
		return true, v.baseTime, nil
	}

	duration, err := parseDuration(parameter)
	if err != nil {
		return false, nil, fmt.Errorf("error parsing duration '%s': %w", parameter, err)
	}

	return true, v.baseTime.Add(time.Duration(duration)), nil
}

type ValueTimeOption func(*ValueBaseTime)

func WithValueTimeTypeName(typeName string) ValueTimeOption {
	return func(v *ValueBaseTime) {
		v.typeName = typeName
	}
}
