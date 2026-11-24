package query

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"time"
)

const (
	listTokenVersion = 1
	// maxTokenLen bounds the work spent on a client-supplied token.
	maxTokenLen = 8 << 10
)

// listToken is the List continuation state: the last key returned plus
// every request parameter that shapes the listing, so a token cannot be
// replayed against a different listing. It is base64url JSON, not signed:
// a forged token can only move the cursor within data the caller may read
// anyway, and every field is validated, so a bad token is a 400.
type listToken struct {
	V              int       `json:"v"`
	Resource       string    `json:"r"`
	Namespace      string    `json:"ns,omitempty"`
	At             time.Time `json:"at"`
	Selector       string    `json:"sel,omitempty"`
	IncludeDeleted bool      `json:"del,omitempty"`
	AfterNamespace string    `json:"kns,omitempty"`
	AfterName      string    `json:"kn"`
}

func encodeListToken(t listToken) string {
	b, err := json.Marshal(t)
	if err != nil {
		// Only strings, a bool, an int and a time: cannot fail.
		panic("encode list token: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeListToken(s string) (listToken, error) {
	if len(s) > maxTokenLen {
		return listToken{}, invalidf("continue token is too long")
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return listToken{}, invalidf("continue token is malformed")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var t listToken
	if err := dec.Decode(&t); err != nil || dec.More() {
		return listToken{}, invalidf("continue token is malformed")
	}
	if t.V != listTokenVersion {
		return listToken{}, invalidf("continue token has unsupported version %d", t.V)
	}
	if t.AfterName == "" {
		return listToken{}, invalidf("continue token is malformed")
	}
	return t, nil
}

// check rejects a token issued for a different listing.
func (t listToken) check(req ListRequest, at time.Time) error {
	switch {
	case t.Resource != req.Resource:
		return invalidf("continue token was issued for resource %q", t.Resource)
	case t.Namespace != req.Namespace:
		return invalidf("continue token was issued for namespace %q", t.Namespace)
	case !t.At.Equal(at):
		return invalidf("continue token was issued for at=%s; pass the same at", t.At.Format(time.RFC3339Nano))
	case t.Selector != req.LabelSelector:
		return invalidf("continue token was issued for a different labelSelector")
	case t.IncludeDeleted != req.IncludeDeleted:
		return invalidf("continue token was issued with includeDeleted=%t", t.IncludeDeleted)
	}
	return nil
}
