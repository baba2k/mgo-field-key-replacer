package replace

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func ReplaceFieldKeysInDocument(a map[string]interface{}, replaceMap map[string]string) (map[string]interface{}, int64) {
	newMap := map[string]interface{}{}
	var modifiedCount int64
	for k, v := range a {
		newKey := k
		if len(replaceMap[k]) > 0 {
			newKey = replaceMap[k]
			modifiedCount += 1
		}
		switch v.(type) {
		case map[string]interface{}:
			newVal, count := ReplaceFieldKeysInDocument(v.(map[string]interface{}), replaceMap)
			newMap[newKey] = newVal
			modifiedCount += count
		case primitive.A:
			v := v.(primitive.A)
			for i, val := range v {
				switch val.(type) {
				case map[string]interface{}:
					newVal, count := ReplaceFieldKeysInDocument(val.(map[string]interface{}), replaceMap)
					v[i] = newVal
					modifiedCount += count
				}
			}
			newMap[newKey] = a[k]
		default:
			newMap[newKey] = a[k]
		}
	}
	return newMap, modifiedCount
}
