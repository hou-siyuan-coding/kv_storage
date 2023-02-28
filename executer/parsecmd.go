package executer

import (
	"errors"
	"kv_storage/datastruct/sortedset"
	"kv_storage/entity"
	"strconv"
	"strings"
)

// list
func prePush(args [][]byte) ([][]byte, error) {
	if len(args) < 3 {
		return nil, errors.New(MissParamErr)
	}
	values := make([][]byte, len(args)-2)
	for i := 2; i < len(args); i++ {
		values[i-2] = args[i]
	}
	return values, nil
}

func preLrange(args [][]byte) (key string, start, stop int, err error) {
	if len(args) < 4 {
		err = errors.New(MissParamErr)
		return
	}
	key = string(args[1])
	if start, err = strconv.Atoi(string(args[2])); err != nil {
		return
	}
	if stop, err = strconv.Atoi(string(args[3])); err != nil {
		return
	}
	return
}

func preLset(args [][]byte) (key string, index int, value string, err error) {
	if len(args) < 4 {
		err = errors.New(MissParamErr)
		return
	}
	key = string(args[1])
	if index, err = strconv.Atoi(string(args[2])); err != nil {
		return
	}
	value = string(args[3])
	return
}

func preLindex(args [][]byte) (string, int, error) {
	if len(args) < 3 {
		return "", 0, errors.New(MissParamErr)
	}
	if index, err := strconv.Atoi(string(args[2])); err != nil {
		return "", 0, err
	} else {
		return string(args[1]), index, nil
	}
}

func prePop(args [][]byte) (string, int, error) {
	if len(args) < 2 {
		return "", 0, errors.New(MissParamErr)
	}
	if len(args) == 2 {
		return string(args[1]), 1, nil
	}
	if index, err := strconv.Atoi(string(args[2])); err != nil {
		return "", 0, err
	} else {
		return string(args[1]), index, nil
	}
}

func preLinsert(args [][]byte) (key string, before bool, pivot, value string, err error) {
	if len(args) < 5 {
		err = errors.New(MissParamErr)
		return
	}
	key = string(args[1])
	position := string(args[2])
	if strings.EqualFold(position, "before") {
		before = true
	} else if !strings.EqualFold(position, "after") {
		err = errors.New(ParamUncorrect)
		return
	}
	pivot, value = string(args[3]), string(args[4])
	return
}

func preLrem(args [][]byte) (key string, count int, value string, err error) {
	if len(args) < 4 {
		err = errors.New(MissParamErr)
		return
	}
	key = string(args[1])
	if count, err = strconv.Atoi(string(args[2])); err != nil {
		return
	}
	value = string(args[3])
	return
}

// sortedSet
func preZadd(args [][]byte) (names []string, scores []float64, err error) {
	if len(args) < 4 {
		err = errors.New(MissParamErr)
		return
	}
	valueNum := (len(args) - 2) / 2
	names = make([]string, valueNum)
	scores = make([]float64, valueNum)
	for i, j := 2, 0; i < len(args)-1; j++ {
		var num float64
		if num, err = strconv.ParseFloat(string(args[i]), 64); err != nil {
			return
		}
		names[j] = string(args[i+1])
		scores[j] = float64(num)
		i += 2
	}
	return
}

func preZrevrange(args [][]byte) (start, stop int, withScore bool, err error) {
	if len(args) < 4 {
		err = errors.New(MissParamErr)
		return
	}
	if start, err = strconv.Atoi(string(args[2])); err != nil {
		return
	}
	if stop, err = strconv.Atoi(string(args[3])); err != nil {
		return
	}
	if len(args) == 5 && strings.EqualFold(string(args[4]), "withscores") {
		withScore = true
	}
	return
}

func preZrankByScore(args [][]byte) (key string, min, max *sortedset.ScoreBorder, withScore bool, offset, count int, err error) {
	argsNum := len(args)
	if argsNum >= 4 {
		key = string(args[1])
		if min, err = sortedset.ParseScoreBorder(string(args[2])); err != nil {
			err = entity.MakeErrReply(err.Error())
			return
		}
		if max, err = sortedset.ParseScoreBorder(string(args[3])); err != nil {
			err = entity.MakeErrReply(err.Error())
			return
		}
	} else {
		err = entity.MakeErrReply(MissParamErr)
		return
	}
	if argsNum == 4 {
		return
	}
	args4 := string(args[4])
	if argsNum == 5 {
		if strings.EqualFold(args4, "withscores") {
			withScore = true
		} else {
			err = entity.MakeErrReply(ParamUncorrect)
		}
		return
	} 
	args5 := string(args[5])
	if argsNum == 7 {
		if strings.EqualFold(args4, "limit") {
			if offset, err = strconv.Atoi(string(args[5])); err != nil {
				return
			}
			if count, err = strconv.Atoi(string(args[6])); err != nil {
				return
			}
		} else {
			err = errors.New(ParamUncorrect)
		}
	} else if argsNum == 8 {
		if strings.EqualFold(args4, "withscores") {
			withScore = true
		} else {
			err = entity.MakeErrReply(ParamUncorrect)
			return
		}
		if strings.EqualFold(args5, "limit") {
			if offset, err = strconv.Atoi(string(args[6])); err != nil {
				return
			}
			if count, err = strconv.Atoi(string(args[7])); err != nil {
				return
			}
		} else {
			err = entity.MakeErrReply(ParamUncorrect)
		}
	} else {
		err = entity.MakeErrReply(ParamUncorrect)
	}
	return
}

func GetKey(args [][]byte) (key string, isHeartbeat bool) {
	if len(args) == 1 && string(args[0]) == "ping" {
		isHeartbeat = true
	} else if len(args) >= 2 {
		key = string(args[1])
	}
	return
}