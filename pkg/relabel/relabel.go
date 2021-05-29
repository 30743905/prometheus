// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package relabel

import (
	"crypto/md5"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"os"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/pkg/labels"
)

var (
	relabelTarget = regexp.MustCompile(`^(?:(?:[a-zA-Z_]|\$(?:\{\w+\}|\w+))+\w*)+$`)

	DefaultRelabelConfig = Config{
		Action:      Replace,
		Separator:   ";",
		Regex:       MustNewRegexp("(.*)"),
		Replacement: "$1",
	}
)

// Action is the action to be performed on relabeling.
type Action string

const (
	// Replace performs a regex replacement.
	Replace Action = "replace"
	// Keep drops targets for which the input does not match the regex.
	Keep Action = "keep"
	// Drop drops targets for which the input does match the regex.
	Drop Action = "drop"
	// HashMod sets a label to the modulus of a hash of labels.
	HashMod Action = "hashmod"
	// LabelMap copies labels to other labelnames based on a regex.
	LabelMap Action = "labelmap"
	// LabelDrop drops any label matching the regex.
	LabelDrop Action = "labeldrop"
	// LabelKeep drops any label not matching the regex.
	LabelKeep Action = "labelkeep"
)

var(
	logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	//level.Info(logger).Log("-------++--->>>>", val)
	//logger1 = log.With(logger1, "ts", log.DefaultTimestamp)
	//logger1 = log.With(logger1, "caller", log.DefaultCaller)
)

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (a *Action) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	switch act := Action(strings.ToLower(s)); act {
	case Replace, Keep, Drop, HashMod, LabelMap, LabelDrop, LabelKeep:
		*a = act
		return nil
	}
	return errors.Errorf("unknown relabel action %q", s)
}

// Config is the configuration for relabeling of target label sets.
type Config struct {
	// A list of labels from which values are taken and concatenated
	// with the configured separator in order.
	SourceLabels model.LabelNames `yaml:"source_labels,flow,omitempty"`
	// Separator is the string between concatenated values from the source labels.
	Separator string `yaml:"separator,omitempty"`
	// Regex against which the concatenation is matched.
	Regex Regexp `yaml:"regex,omitempty"`
	// Modulus to take of the hash of concatenated values from the source labels.
	Modulus uint64 `yaml:"modulus,omitempty"`
	// TargetLabel is the label to which the resulting string is written in a replacement.
	// Regexp interpolation is allowed for the replace action.
	TargetLabel string `yaml:"target_label,omitempty"`
	// Replacement is the regex replacement pattern to be used.
	Replacement string `yaml:"replacement,omitempty"`
	// Action is the action to be performed for the relabeling.
	Action Action `yaml:"action,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultRelabelConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	if c.Regex.Regexp == nil {
		c.Regex = MustNewRegexp("")
	}
	if c.Modulus == 0 && c.Action == HashMod {
		return errors.Errorf("relabel configuration for hashmod requires non-zero modulus")
	}
	if (c.Action == Replace || c.Action == HashMod) && c.TargetLabel == "" {
		return errors.Errorf("relabel configuration for %s action requires 'target_label' value", c.Action)
	}
	if c.Action == Replace && !relabelTarget.MatchString(c.TargetLabel) {
		return errors.Errorf("%q is invalid 'target_label' for %s action", c.TargetLabel, c.Action)
	}
	if c.Action == LabelMap && !relabelTarget.MatchString(c.Replacement) {
		return errors.Errorf("%q is invalid 'replacement' for %s action", c.Replacement, c.Action)
	}
	if c.Action == HashMod && !model.LabelName(c.TargetLabel).IsValid() {
		return errors.Errorf("%q is invalid 'target_label' for %s action", c.TargetLabel, c.Action)
	}

	if c.Action == LabelDrop || c.Action == LabelKeep {
		if c.SourceLabels != nil ||
			c.TargetLabel != DefaultRelabelConfig.TargetLabel ||
			c.Modulus != DefaultRelabelConfig.Modulus ||
			c.Separator != DefaultRelabelConfig.Separator ||
			c.Replacement != DefaultRelabelConfig.Replacement {
			return errors.Errorf("%s action requires only 'regex', and no other fields", c.Action)
		}
	}

	return nil
}

// Regexp encapsulates a regexp.Regexp and makes it YAML marshalable.
type Regexp struct {
	*regexp.Regexp
	original string
}

// NewRegexp creates a new anchored Regexp and returns an error if the
// passed-in regular expression does not compile.
func NewRegexp(s string) (Regexp, error) {
	regex, err := regexp.Compile("^(?:" + s + ")$")
	return Regexp{
		Regexp:   regex,
		original: s,
	}, err
}

// MustNewRegexp works like NewRegexp, but panics if the regular expression does not compile.
func MustNewRegexp(s string) Regexp {
	re, err := NewRegexp(s)
	if err != nil {
		panic(err)
	}
	return re
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (re *Regexp) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	r, err := NewRegexp(s)
	if err != nil {
		return err
	}
	*re = r
	return nil
}

// MarshalYAML implements the yaml.Marshaler interface.
func (re Regexp) MarshalYAML() (interface{}, error) {
	if re.original != "" {
		return re.original, nil
	}
	return nil, nil
}

// Process returns a relabeled copy of the given label set. The relabel configurations
// are applied in order of input.
// If a label set is dropped, nil is returned.
// May return the input labelSet modified.
func Process(labels labels.Labels, cfgs ...*Config) labels.Labels {
	for _, cfg := range cfgs {
		labels = relabel(labels, cfg)
		if labels == nil {
			return nil
		}
	}
	return labels
}



/**
	relabel_configs场景：
	lset：job、__scheme__、__metrics_path__、__address__
	cfg: target下配置的relabel_configs其中一条规则配置

    metric_relabel_configs场景：
	lset：instance、job、__name__、指标标签
	cfg: target下配置的metric_relabel_configs其中一条规则配置
 */
func relabel(lset labels.Labels, cfg *Config) labels.Labels {
	values := make([]string, 0, len(cfg.SourceLabels))
	for _, ln := range cfg.SourceLabels {
		values = append(values, lset.Get(string(ln)))
	}
	val := strings.Join(values, cfg.Separator)

	//fmt.Println("-------++--->>>>", val)

	logger = log.With(logger, "ts", log.DefaultTimestamp)
	logger = log.With(logger, "caller", log.DefaultCaller)
	level.Info(logger).Log("-------++--->>>>", val)
	//logger = log.With(logger, "ts", log.DefaultTimestamp)
	//logger = log.With(logger, "caller", log.DefaultCaller)



	lb := labels.NewBuilder(lset)

	switch cfg.Action {
	case Drop:
		if cfg.Regex.MatchString(val) {
			return nil
		}
	case Keep:
		if !cfg.Regex.MatchString(val) {
			return nil
		}
	case Replace:
		indexes := cfg.Regex.FindStringSubmatchIndex(val)
		// If there is no match no replacement must take place.
		if indexes == nil {
			break
		}
		// 解析target_label，如target_label: test011_$2
		target := model.LabelName(cfg.Regex.ExpandString([]byte{}, cfg.TargetLabel, val, indexes))
		if !target.IsValid() {
			lb.Del(cfg.TargetLabel)
			break
		}
		// 解析replacement配置，如：replacement: test_$1
		res := cfg.Regex.ExpandString([]byte{}, cfg.Replacement, val, indexes)
		if len(res) == 0 {
			lb.Del(cfg.TargetLabel)
			break
		}
		//target_label设置解析值
		lb.Set(string(target), string(res))
		// 将target_label设置为串联的source_labels的哈希的模数。
	//relabel_configs:
	//	- source_labels: [ '__address__' ]
	//    modulus: 10
	//    target_label: tmp_hash
	//	  action: hashmod
	case HashMod:
		mod := sum64(md5.Sum([]byte(val))) % cfg.Modulus
		lb.Set(cfg.TargetLabel, fmt.Sprintf("%d", mod))
		/**
		labelmap会根据regex去匹配Target实例所有标签的名称（注意是名称），并且将捕获到的内容作为为新的标签名称，
		regex匹配到标签的的值作为新标签的值。

		labelmap: 将正则表达式与所有标签名称匹配。 然后，将匹配标签的值复制到通过替换为它们的值替换的匹配组引用(${1}, ${2}, ...)给出的标签名称。

		例如：
		relabel_configs:
		- action: labelmap
		  regex: __meta_kubernetes_node_label_(.+)
		原标签为： __meta_kubernetes_node_label_test=tttt
		则目标标签为： test=tttt
		 */
	case LabelMap:
		for _, l := range lset {
			if cfg.Regex.MatchString(l.Name) {
				res := cfg.Regex.ReplaceAllString(l.Name, cfg.Replacement)
				lb.Set(res, l.Value)
			}
		}
		/**
		labeldrop:
			regex配置正则匹配标签名称，匹配上则删除该label
		例如：
		relabel_configs:
		  - regex: label_should_drop_(.+)
		    action: labeldrop
		 */
	case LabelDrop:
		for _, l := range lset {
			if cfg.Regex.MatchString(l.Name) {
				lb.Del(l.Name)
			}
		}
		/**
		labelkeep:
			regex配置正则匹配标签名称，匹配上则保留该label，没有匹配上的则删除
		 */
	case LabelKeep:
		for _, l := range lset {
			if !cfg.Regex.MatchString(l.Name) {
				lb.Del(l.Name)
			}
		}
	default:
		panic(errors.Errorf("relabel: unknown relabel action type %q", cfg.Action))
	}

	return lb.Labels()
}

// sum64 sums the md5 hash to an uint64.
func sum64(hash [md5.Size]byte) uint64 {
	var s uint64

	for i, b := range hash {
		shift := uint64((md5.Size - i - 1) * 8)

		s |= uint64(b) << shift
	}
	return s
}
