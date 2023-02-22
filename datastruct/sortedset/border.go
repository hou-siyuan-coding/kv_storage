package sortedset

import (
	"errors"
	"strconv"
)

const (
	negativeInf int8 = -1
	positiveInf int8 = 1
)

// 表示分数范围的边界，可表示： <, <=, >, >=, +inf(inf), -inf
type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

// 正无穷
var positiveInfBorder = &ScoreBorder{
	Inf: positiveInf,
}

// 负无穷
var negativeInfBorder = &ScoreBorder{
	Inf: negativeInf,
}

func (border *ScoreBorder) greater(value float64) bool {
	if border.Inf == negativeInf {
		return false
	} else if border.Inf == positiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

func (border *ScoreBorder) less(value float64) bool {
	if border.Inf == negativeInf {
		return true
	} else if border.Inf == positiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

func ParseScoreBorder(s string) (*ScoreBorder, error) {
	if s == "inf" || s == "+inf" {
		return positiveInfBorder, nil
	}
	if s == "-inf" {
		return negativeInfBorder, nil
	}
	if s[0] == '(' {
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, errors.New("ERR min or max is not a float")
		}
		return &ScoreBorder{
			Inf:     0,
			Value:   value,
			Exclude: true,
		}, nil
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, errors.New("ERR min or max is not a float")
	}
	return &ScoreBorder{
		Inf:     0,
		Value:   value,
		Exclude: false,
	}, nil
}
