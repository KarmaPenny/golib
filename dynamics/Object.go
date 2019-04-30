package dynamics

type Object map[string]interface{}

// HasField returns true if the field is in the object
func (self Object) HasField(field string) bool {
	_, ok := self[field]
	return ok
}

// GetArray attempts to get a field as an Array. If the field does not exist or is not an Array the fallback value is returned.
func (self Object) GetArray(field string, fallback Array) Array {
	if self.HasField(field) {
		if value, ok := self[field].([]interface{}); ok {
			return Array(value)
		}
	}
	return fallback
}

// GetBool attempts to get a field as a bool. If the field does not exist or is not a bool the fallback value is returned.
func (self Object) GetBool(field string, fallback bool) bool {
	if self.HasField(field) {
		if value, ok := self[field].(bool); ok {
			return value
		}
	}
	return fallback
}

// GetInt attempts to get a field as an int. If the field does not exist or is not an int the fallback value is returned.
func (self Object) GetInt(field string, fallback int) int {
	if self.HasField(field) {
		if value, ok := self[field].(int); ok {
			return value
		}
	}
	return fallback
}

// GetString attempts to get a field as a string. If the field does not exist or is not a string the fallback value is returned.
func (self Object) GetString(field string, fallback string) string {
	if self.HasField(field) {
		if value, ok := self[field].(string); ok {
			return value
		}
	}
	return fallback
}
