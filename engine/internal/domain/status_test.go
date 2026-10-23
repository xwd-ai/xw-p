package domain

import "testing"

// 验证消息状态机的合法转移路径、终态行为与解析往返。

func TestStatusHappyPath(t *testing.T) {
	cases := [][2]MessageStatus{
		{StatusCreated, StatusQueued},
		{StatusQueued, StatusProcessing},
		{StatusProcessing, StatusSent},
	}
	for _, c := range cases {
		if !c[0].CanTransitionTo(c[1]) {
			t.Errorf("%s -> %s 应为合法", c[0], c[1])
		}
	}
}

func TestStatusAllowsFailureShortCircuit(t *testing.T) {
	cases := [][2]MessageStatus{
		{StatusCreated, StatusFailed},
		{StatusQueued, StatusFailed},
		{StatusProcessing, StatusFailed},
	}
	for _, c := range cases {
		if !c[0].CanTransitionTo(c[1]) {
			t.Errorf("%s -> %s 应为合法", c[0], c[1])
		}
	}
}

func TestStatusAllowsRetryBackToQueued(t *testing.T) {
	if !StatusProcessing.CanTransitionTo(StatusQueued) {
		t.Error("processing -> queued 应为合法（投递失败重试回滚）")
	}
}

func TestStatusRejectsIllegalTransitions(t *testing.T) {
	cases := [][2]MessageStatus{
		{StatusCreated, StatusSent},
		{StatusCreated, StatusProcessing},
		{StatusQueued, StatusCreated},
		{StatusQueued, StatusSent},
	}
	for _, c := range cases {
		if c[0].CanTransitionTo(c[1]) {
			t.Errorf("%s -> %s 应为非法", c[0], c[1])
		}
	}
	if StatusCreated.CanTransitionTo("") {
		t.Error("空目标状态应为非法")
	}
}

func TestStatusTerminalHaveNoOutgoing(t *testing.T) {
	for _, terminal := range []MessageStatus{StatusSent, StatusFailed} {
		if !terminal.IsTerminal() {
			t.Errorf("%s 应为终态", terminal)
		}
		for _, next := range AllStatuses() {
			if terminal.CanTransitionTo(next) {
				t.Errorf("%s -> %s 应为非法", terminal, next)
			}
		}
	}
}

func TestStatusNonTerminal(t *testing.T) {
	for _, s := range []MessageStatus{StatusCreated, StatusQueued, StatusProcessing} {
		if s.IsTerminal() {
			t.Errorf("%s 不应为终态", s)
		}
	}
}

func TestStatusParseRoundTrip(t *testing.T) {
	if s, ok := ParseStatus("created"); !ok || s != StatusCreated {
		t.Errorf("created 解析失败: %v %v", s, ok)
	}
	if s, ok := ParseStatus("sent"); !ok || s != StatusSent {
		t.Errorf("sent 解析失败: %v %v", s, ok)
	}
	if _, ok := ParseStatus("unknown"); ok {
		t.Error("unknown 不应解析成功")
	}
}
