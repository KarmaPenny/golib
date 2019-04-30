package elk

type Analysis struct {
	Type string `json:"type"`
	Version string `json:"version"`
	LastRunTime string `json:"last_run_time"`
	Data []byte `json:"data"`
	Observables []string `json:"observables"`
	Tags []string `json:"tags"`
	new_observables map[string]struct{}
	new_tags map[string]struct{}
}

func NewAnalysis(analysis_type string, version string, last_run_time string) (*Analysis) {
	analysis := Analysis{
		Type: analysis_type,
		Version: version,
		LastRunTime: last_run_time,
		Observables: []string{},
		Tags: []string{},
	}
	analysis.new_observables = map[string]struct{}{}
	analysis.new_tags = map[string]struct{}{}
	return &analysis
}

func (self *Analysis) AddObservable(observable string) {
	if _, ok := self.new_observables[observable]; !ok {
		self.new_observables[observable] = struct{}{}
		self.Observables = append(self.Observables, observable)
	}
}

func (self *Analysis) AddTag(tag string) {
	if _, ok := self.new_tags[tag]; !ok {
		self.new_tags[tag] = struct{}{}
		self.Tags = append(self.Tags, tag)
	}
}
