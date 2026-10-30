// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	tagPlaceholder = "$TAG"
	maxBigQueryID  = 1024 // maximum length in bytes of a BigQuery dataset or table ID
)

// templatePart is one segment of a tagTemplate: either literal text or a tag reference.
// tagIndex is -1 for the whole tag ($TAG) and n for $TAG[n].
type templatePart struct {
	literal  string
	isTag    bool
	tagIndex int
}

// tagTemplate is a parsed TableId / DatasetId value. It supports the same placeholders as
// Fluent Bit's out_s3 "s3_key_format": "$TAG" (whole tag) and "$TAG[n]" (n-th 0-based part
// of the tag split on the configured delimiters). Everything else is literal text.
type tagTemplate struct {
	raw   string
	parts []templatePart
}

// parseTagTemplate parses s into a tagTemplate. A value without placeholders yields a
// template whose dynamic() is false and whose render() always returns s.
func parseTagTemplate(s string) (*tagTemplate, error) {
	t := &tagTemplate{raw: s}
	var literal strings.Builder
	rest := s
	for {
		idx := strings.Index(rest, tagPlaceholder)
		if idx < 0 {
			literal.WriteString(rest)
			break
		}
		literal.WriteString(rest[:idx])
		rest = rest[idx+len(tagPlaceholder):]

		part := templatePart{isTag: true, tagIndex: -1}
		if strings.HasPrefix(rest, "[") {
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				return nil, fmt.Errorf("unterminated $TAG[ in %q", s)
			}
			n, err := strconv.Atoi(rest[1:end])
			if err != nil || n < 0 {
				return nil, fmt.Errorf("invalid $TAG index %q in %q", rest[1:end], s)
			}
			part.tagIndex = n
			rest = rest[end+1:]
		}
		if literal.Len() > 0 {
			t.parts = append(t.parts, templatePart{literal: literal.String()})
			literal.Reset()
		}
		t.parts = append(t.parts, part)
	}
	if literal.Len() > 0 || len(t.parts) == 0 {
		t.parts = append(t.parts, templatePart{literal: literal.String()})
	}
	return t, nil
}

// dynamic reports whether the template references the tag.
func (t *tagTemplate) dynamic() bool {
	for _, p := range t.parts {
		if p.isTag {
			return true
		}
	}
	return false
}

// render substitutes the tag into the template. delimiters is the set of characters on which
// the tag is split for $TAG[n]; empty parts between consecutive delimiters are dropped.
func (t *tagTemplate) render(tag string, delimiters string) (string, error) {
	if !t.dynamic() {
		return t.raw, nil
	}
	var tagParts []string
	var b strings.Builder
	for _, p := range t.parts {
		switch {
		case !p.isTag:
			b.WriteString(p.literal)
		case p.tagIndex < 0:
			b.WriteString(tag)
		default:
			if tagParts == nil {
				tagParts = strings.FieldsFunc(tag, func(r rune) bool { return strings.ContainsRune(delimiters, r) })
			}
			if p.tagIndex >= len(tagParts) {
				return "", fmt.Errorf("tag %q has %d part(s); $TAG[%d] in %q is out of range", tag, len(tagParts), p.tagIndex, t.raw)
			}
			b.WriteString(tagParts[p.tagIndex])
		}
	}
	return b.String(), nil
}

// validTableID reports whether id satisfies BigQuery table naming rules: Unicode letters,
// marks, numbers, connector punctuation (including "_"), dashes and spaces, up to 1024 bytes.
func validTableID(id string) bool {
	if id == "" || len(id) > maxBigQueryID || !utf8.ValidString(id) {
		return false
	}
	for _, r := range id {
		if !(unicode.IsLetter(r) || unicode.IsMark(r) || unicode.IsNumber(r) ||
			unicode.Is(unicode.Pc, r) || unicode.Is(unicode.Pd, r) || unicode.Is(unicode.Zs, r)) {
			return false
		}
	}
	return true
}

// validDatasetID reports whether id satisfies BigQuery dataset naming rules: ASCII letters,
// digits and underscores, up to 1024 bytes.
func validDatasetID(id string) bool {
	if id == "" || len(id) > maxBigQueryID {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !(c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			return false
		}
	}
	return true
}
