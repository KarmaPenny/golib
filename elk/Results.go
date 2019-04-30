package elk

type BulkResults struct {
	Took int `json:"took"`
	Errors bool `json:"errors"`
	Items []OperationResults `json:"items"`
}

type OperationResults struct {
	Index Document `json:"index"`
	Delete Document `json:"index"`
	Create Document `json:"index"`
	Update Document `json:"index"`
}

type MultiSearchResults struct {
	Responses []SearchResults `json:"responses"`
}

type SearchResults struct {
	Took int `json:"took"`
	TimedOut bool `json:"timed_out"`
	Shards Shards `json:"_shards"`
	Hits Hits `json:"hits"`
}

type Shards struct {
	Total int `json:"total"`
	Successful int `json:"successful"`
	Skipped int `json:"skipped"`
	Failed int `json:"failed"`
}

type Hits struct {
	Total int `json:"total"`
	MaxScore float32 `json:"max_score"`
	Hits []Document `json:"hits"`
}

type IndexSettingsResults map[string]SettingsResults

type SettingsResults struct {
	Settings Settings `json:"settings"`
}

type Settings struct {
	Index IndexSettings `json:"index"`
}

type IndexSettings struct {
	RefreshInterval string `json:"refresh_interval"`
}

type UpdateByQueryResults struct {
	Took int `json:"took"`
	TimedOut bool `json:"timed_out"`
	Total int `json:"total"`
	Updated int `json:"updated"`
	Deleted int `json:"deleted"`
	Batches int `json:"batches"`
	VersionConflicts int `json:"version_conflicts"`
	Noops int `json:"noops"`
	Retries Retries `json:"retries"`
	ThrottledMillis int `json:"throttled_millis"`
	RequestsPerSecond float64 `json:"requests_per_second"`
	ThrottledUntilMillis int `json:"throttled_until_millis"`
	Failures []interface{} `json:"failures"`
}

type Retries struct {
	Bulk int `json:"bulk"`
	Search int `json:"search"`
}
