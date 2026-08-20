package keelstore

import "testing"

func TestInternalKeyRoundTrip(t *testing.T) {
	ik := makeInternalKey([]byte("hello"), 42, kindValue)
	if got := string(userKeyOf(ik)); got != "hello" {
		t.Fatalf("userKeyOf = %q", got)
	}
	if got := seqOf(ik); got != 42 {
		t.Fatalf("seqOf = %d", got)
	}
	if got := kindOf(ik); got != kindValue {
		t.Fatalf("kindOf = %d", got)
	}
	if len(ik) != len("hello")+trailerLen {
		t.Fatalf("internal key is %d bytes, want %d", len(ik), len("hello")+trailerLen)
	}
}

func TestInternalKeyOrdersUserKeyThenNewestSequenceFirst(t *testing.T) {
	cases := []struct {
		name string
		a, b []byte
		want int
	}{
		{"smaller user key first", makeInternalKey([]byte("a"), 1, kindValue), makeInternalKey([]byte("b"), 1, kindValue), -1},
		{"larger user key last", makeInternalKey([]byte("b"), 9, kindValue), makeInternalKey([]byte("a"), 9, kindValue), 1},
		{"newer sequence first", makeInternalKey([]byte("a"), 9, kindValue), makeInternalKey([]byte("a"), 1, kindValue), -1},
		{"older sequence last", makeInternalKey([]byte("a"), 1, kindValue), makeInternalKey([]byte("a"), 9, kindValue), 1},
		{"value before tombstone at one sequence", makeInternalKey([]byte("a"), 5, kindValue), makeInternalKey([]byte("a"), 5, kindDelete), -1},
		{"identical", makeInternalKey([]byte("a"), 5, kindValue), makeInternalKey([]byte("a"), 5, kindValue), 0},
		{"prefix sorts before extension", makeInternalKey([]byte("ab"), 1, kindValue), makeInternalKey([]byte("abc"), 1, kindValue), -1},
	}
	for _, c := range cases {
		if got := compareInternalKeys(c.a, c.b); got != c.want {
			t.Errorf("%s: compare = %d, want %d", c.name, got, c.want)
		}
	}
}

// A seek for (key, snapshotSeq) must land on the newest version the
// snapshot may see, skipping anything newer.
func TestSeekKeySkipsNewerVersions(t *testing.T) {
	newer := makeInternalKey([]byte("k"), 10, kindValue)
	target := seekKey([]byte("k"), 5)
	older := makeInternalKey([]byte("k"), 3, kindValue)

	if compareInternalKeys(newer, target) >= 0 {
		t.Fatal("a version newer than the snapshot does not sort before the seek target")
	}
	if compareInternalKeys(older, target) <= 0 {
		t.Fatal("a version older than the snapshot does not sort after the seek target")
	}
}

func TestFirstKeyForSortsBeforeEveryVersion(t *testing.T) {
	bound := firstKeyFor([]byte("k"))
	for _, seq := range []uint64{0, 1, 1000, maxSequence} {
		for _, kind := range []uint8{kindDelete, kindValue} {
			if compareInternalKeys(bound, makeInternalKey([]byte("k"), seq, kind)) > 0 {
				t.Fatalf("lower bound sorts after (k, %d, %d)", seq, kind)
			}
		}
	}
	if compareInternalKeys(bound, makeInternalKey([]byte("j"), 1, kindValue)) <= 0 {
		t.Fatal("lower bound for k must sort after every version of j")
	}
}

func TestValidInternalKey(t *testing.T) {
	if validInternalKey(make([]byte, trailerLen)) {
		t.Error("a key with no user portion is not valid")
	}
	if !validInternalKey(makeInternalKey([]byte("a"), 1, kindValue)) {
		t.Error("a normal internal key was rejected")
	}
	if userKeyOf(nil) != nil || trailerOf(nil) != 0 {
		t.Error("accessors on a too-short key should degrade quietly")
	}
}
