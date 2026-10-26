package domain

import "testing"

func TestAllStepTypesCoversSeven(t *testing.T) {
	got := map[StepType]bool{}
	for _, s := range AllStepTypes() {
		got[s] = true
	}
	want := []StepType{
		StepTypeInApp, StepTypeEmail, StepTypeSMS, StepTypePush,
		StepTypeChat, StepTypeDelay, StepTypeDigest,
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("AllStepTypes 缺少 %q", w)
		}
	}
	if len(AllStepTypes()) != len(want) {
		t.Errorf("AllStepTypes 长度 = %d, 期望 %d", len(AllStepTypes()), len(want))
	}
}

func TestDeliveryAndOrchestrationPartition(t *testing.T) {
	delivery := map[StepType]bool{}
	for _, s := range DeliveryStepTypes() {
		delivery[s] = true
	}
	for _, s := range OrchestrationStepTypes() {
		if delivery[s] {
			t.Errorf("编排步骤 %q 不应出现在投递类型中", s)
		}
	}
	if len(DeliveryStepTypes()) != 5 {
		t.Errorf("投递类型数量 = %d, 期望 5", len(DeliveryStepTypes()))
	}
	if len(OrchestrationStepTypes()) != 2 {
		t.Errorf("编排类型数量 = %d, 期望 2", len(OrchestrationStepTypes()))
	}
}

func TestStepTypeIsDelivery(t *testing.T) {
	cases := []struct {
		t    StepType
		want bool
	}{
		{StepTypeInApp, true},
		{StepTypeEmail, true},
		{StepTypeSMS, true},
		{StepTypePush, true},
		{StepTypeChat, true},
		{StepTypeDelay, false},
		{StepTypeDigest, false},
	}
	for _, c := range cases {
		if got := c.t.IsDelivery(); got != c.want {
			t.Errorf("%q.IsDelivery() = %v, 期望 %v", c.t, got, c.want)
		}
	}
}

func TestStepTypeChannelMapping(t *testing.T) {
	if ch, ok := StepTypeEmail.Channel(); !ok || ch != ChannelEmail {
		t.Errorf("StepTypeEmail.Channel() = %q, %v", ch, ok)
	}
	if ch, ok := StepTypeSMS.Channel(); !ok || ch != ChannelSMS {
		t.Errorf("StepTypeSMS.Channel() = %q, %v", ch, ok)
	}
	// 编排步骤无渠道
	for _, s := range []StepType{StepTypeDelay, StepTypeDigest} {
		if _, ok := s.Channel(); ok {
			t.Errorf("编排步骤 %q 不应有投递渠道", s)
		}
	}
}

func TestFromChannelRoundtrip(t *testing.T) {
	for _, c := range DeliveryChannels() {
		st, ok := FromChannel(c)
		if !ok {
			t.Errorf("FromChannel(%q) 应成功", c)
			continue
		}
		ch, ok := st.Channel()
		if !ok || ch != c {
			t.Errorf("FromChannel(%q) 往返后 = %q, %v", c, ch, ok)
		}
	}
	// 编排渠道（delay/digest 的 Channel 值）不应能反查为投递步骤类型
	if _, ok := FromChannel(ChannelDelay); ok {
		t.Errorf("FromChannel(delay) 不应成功")
	}
}

func TestParseStepType(t *testing.T) {
	if st, ok := ParseStepType("email"); !ok || st != StepTypeEmail {
		t.Errorf("ParseStepType(email) = %q, %v", st, ok)
	}
	if _, ok := ParseStepType("not_a_step"); ok {
		t.Errorf("ParseStepType(not_a_step) 应失败（不在枚举内）")
	}
}
