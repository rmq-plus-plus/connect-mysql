package utils

import (
	"errors"
	"os"
)

func ReadAllFile(filePath string) (string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", errors.New("读取" + filePath + "失败，" + err.Error())
	}
	// 该方式读取后，返回的是一个[]byte
	return string(content), nil
}
