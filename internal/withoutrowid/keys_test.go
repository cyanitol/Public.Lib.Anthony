// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package withoutrowid

import (
	"bytes"
	"testing"
)

func TestEncodeCompositeKey_IntOrdering(t *testing.T) {
	low := EncodeCompositeKey([]interface{}{-5})
	high := EncodeCompositeKey([]interface{}{10})
	if bytes.Compare(low, high) >= 0 {
		t.Fatalf("expected -5 to sort before 10: %x vs %x", low, high)
	}
}

func TestEncodeCompositeKey_FloatOrdering(t *testing.T) {
	low := EncodeCompositeKey([]interface{}{-1.5})
	mid := EncodeCompositeKey([]interface{}{0.0})
	high := EncodeCompositeKey([]interface{}{1.5})
	if !(bytes.Compare(low, mid) < 0 && bytes.Compare(mid, high) < 0) {
		t.Fatalf("float ordering incorrect: low=%x mid=%x high=%x", low, mid, high)
	}
}

func TestEncodeCompositeKey_TextOrdering(t *testing.T) {
	a := EncodeCompositeKey([]interface{}{"apple"})
	b := EncodeCompositeKey([]interface{}{"banana"})
	if bytes.Compare(a, b) >= 0 {
		t.Fatalf("text ordering incorrect: apple should sort before banana")
	}
}

func TestEncodeCompositeKey_CompositeOrdering(t *testing.T) {
	k1 := EncodeCompositeKey([]interface{}{1, "a"})
	k2 := EncodeCompositeKey([]interface{}{1, "b"})
	k3 := EncodeCompositeKey([]interface{}{2, "a"})

	if !(bytes.Compare(k1, k2) < 0 && bytes.Compare(k2, k3) < 0) {
		t.Fatalf("composite ordering incorrect: k1=%x k2=%x k3=%x", k1, k2, k3)
	}
}

func TestEncodeCompositeKey_NullOrdering(t *testing.T) {
	null := EncodeCompositeKey([]interface{}{nil})
	nonNull := EncodeCompositeKey([]interface{}{0})
	if bytes.Compare(null, nonNull) >= 0 {
		t.Fatalf("NULL should sort before non-NULL: null=%x nonNull=%x", null, nonNull)
	}
}
