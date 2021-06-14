package util

import "strings"

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

func GetError(id string) string {
	return id + ":" + id
}
