package util

import (
	"strings"
)

func Contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func GetFieldValueFromMap(m map[string]interface{}, field string) string {
	v, ok := m[field]
	if !ok {
		v, ok = m[strings.ToLower(field)]
		if !ok {
			v, ok = m[strings.ToUpper(field)]
			if !ok {
				v = ""
			}
		}
	}
	return strings.Trim(v.(string), " ")
}

func GetFieldFromMap(m map[string]interface{}, field string) string {
	_, ok := m[field]
	if !ok {
		field = strings.ToLower(field)
		_, ok = m[field]
		if !ok {
			field = strings.ToUpper(field)
			if !ok {
				field = ""
			}
		}
	}
	return field
}

func remove_index(s []string, index int) []string {
	ret := make([]string, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}

func IndexOf(word string, data []string) int {
	if data != nil && len(data) > 0 {
		for k, v := range data {
			if word == v {
				return k
			}
		}
	}
	return -1
}

func RemoveExcludedColumns(columns []string, excluded []string) []string {
	if excluded == nil || len(excluded) > 0 {
		for _, c := range excluded {
			idx := IndexOf(c, columns)
			if idx != -1 {
				columns = remove_index(columns, idx)
			}
		}
	}
	return columns
}
