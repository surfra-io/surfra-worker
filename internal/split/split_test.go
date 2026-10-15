package split

import (
	"reflect"
	"testing"
)

func TestCommand(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{`echo surfra`, []string{"echo", "surfra"}},
		{`vllm serve --model Qwen/Qwen3-0.6B --host 127.0.0.1 --port 8000`, []string{"vllm", "serve", "--model", "Qwen/Qwen3-0.6B", "--host", "127.0.0.1", "--port", "8000"}},
		{`cmd "a b" 'c d'`, []string{"cmd", "a b", "c d"}},
		{`printf %s hello\ world`, []string{"printf", "%s", "hello world"}},
	}
	for _, tc := range cases {
		got, err := Command(tc.in)
		if err != nil {
			t.Fatalf("%q: %v", tc.in, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%q: got %#v", tc.in, got)
		}
	}
}

func TestCommandRejects(t *testing.T) {
	for _, in := range []string{"", "   ", `"open`, `trail\`} {
		if _, err := Command(in); err == nil {
			t.Fatalf("%q: expected an error", in)
		}
	}
}
