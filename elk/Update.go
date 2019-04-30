package elk

import (
	. "github.com/KarmaPenny/golib/dynamics"
	"fmt"
	"strconv"
	"strings"
)

type BulkUpdate map[string]*Update

type Update struct {
	path string
	script strings.Builder
	params Object
}

func NewUpdate(path string) *Update {
	return &Update{path: path}
}

func (self *Update) Action() Object {
	ids := strings.Split(self.path, "/")
	return Object{"update": Object{"_index": ids[1], "_type": ids[2], "_id": ids[3]}}
}

func (self *Update) Source() Object {
	return Object{"script": Object{"source": self.script.String(), "lang": "painless", "params": self.params}}
}

// AddParameter adds a parameter to the update
func (self *Update) AddParameter(parameter interface{}) {
	if self.params == nil {
		self.params = Object{}
	}
	self.params[strconv.Itoa(len(self.params))] = parameter
}

// SetAnalysis sets the analysis of type analysis_type to analysis. If the analysis already exists it is overwritten.
func (self *Update) SetAnalysis(analysis_type string, analysis *Analysis) {
	// add script that creates or overwrites the analysis of type analysis_type
	self.script.WriteString(strings.NewReplacer("\t", "", "\n", "").Replace(fmt.Sprintf(`
		if (ctx._source.analysis == null) {
			ctx._source.analysis = new ArrayList();
			ctx._source.analysis.add(params.%d);
		} else {
			int index = -1;
			for (int i = 0; i < ctx._source.analysis.length; ++i) {
				if (ctx._source.analysis.type == '%s') {
					index = i;
				}
			}
			if (index == -1) {
				ctx._source.analysis.add(params.%d);
			} else {
				ctx._source.analysis[index] = params.%d;
			}
		}
		`,
		len(self.params),
		analysis_type,
		len(self.params),
		len(self.params),
	)))

	// add analysis as parameter
	self.AddParameter(analysis)
}

// CreateField sets the value of a field. If the field already exists the existing value is not replaced. To replace the existing value use SetField.
func (self *Update) CreateField(field string, value string) {
	// add script that sets field value when field does not exist
	self.script.WriteString(strings.NewReplacer("\t", "", "\n", "").Replace(fmt.Sprintf(`
		if (ctx._source.%s == null) {
			ctx._source.%s = params.%d;
		}
		`,
		field,
		field,
		len(self.params),
	)))

	// add new parameter
	self.AddParameter(value)
}

// SetField sets the value of a field. If the field already exists the existing value is replaced. To keep the existing value use CreateField.
func (self *Update) SetField(field string, value string) {
	// add script that sets field value
	self.script.WriteString(strings.NewReplacer("\t", "", "\n", "").Replace(fmt.Sprintf(`
		ctx._source.%s = params.%d;
		`,
		field,
		len(self.params),
	)))

	// add new parameter
	self.AddParameter(value)
}

// AppendField appends a value to an array field without creating duplicates. A new array containing the value is created if the field does not exist.
func (self *Update) AppendField(field string, value string) {
	// add script that appends a value to the array without creating duplicates
	self.script.WriteString(strings.NewReplacer("\t", "", "\n", "").Replace(fmt.Sprintf(`
		if (ctx._source.%s == null) {
			ctx._source.%s = new ArrayList();
			ctx._source.%s.add(params.%d);
		} else if (!ctx._source.%s.contains(params.%d)) {
			ctx._source.%s.add(params.%d);
		}
		`,
		field,
		field,
		field,
		len(self.params),
		field,
		len(self.params),
		field,
		len(self.params),
	)))

	// add new parameter
	self.AddParameter(value)
}
