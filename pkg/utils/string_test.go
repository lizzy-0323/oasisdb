package utils

import (
	"bytes"
	"testing"
)

func TestGetSeparatorBetweenReturnsExistingLeftBound(t *testing.T) {
	left := []byte("left-bound")
	right := []byte("right-bound")

	got := GetSeparatorBetween(left, right)

	if !bytes.Equal(got, left) {
		t.Fatalf("expected %q, got %q", left, got)
	}
	if &got[0] != &left[0] {
		t.Fatal("expected function to reuse the existing left bound slice")
	}
}

func TestGetSeparatorBetweenBuildsCopyFromRightBound(t *testing.T) {
	left := []byte{}
	right := []byte("cat")

	got := GetSeparatorBetween(left, right)

	if !bytes.Equal(got, []byte("cas")) {
		t.Fatalf("expected separator %q, got %q", []byte("cas"), got)
	}
	if !bytes.Equal(right, []byte("cat")) {
		t.Fatalf("expected right bound to remain unchanged, got %q", right)
	}
}

func TestGetSeparatorBetweenHandlesSingleByteRightBound(t *testing.T) {
	got := GetSeparatorBetween(nil, []byte{5})

	if !bytes.Equal(got, []byte{4}) {
		t.Fatalf("expected separator [4], got %v", got)
	}
}
