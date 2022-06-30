package base_func

import "encoding/json"

func Any2String(data interface{}) (string, error) {
	jsonBytes, err := json.Marshal(data); if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}
