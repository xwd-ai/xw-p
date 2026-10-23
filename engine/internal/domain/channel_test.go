package domain

import "testing"

func TestDeliveryChannelsExcludeOrchestration(t *testing.T) {
	got := DeliveryChannels()
	want := map[Channel]bool{
		ChannelInApp: true,
		ChannelEmail: true,
		ChannelSMS:   true,
		ChannelPush:  true,
		ChannelChat:  true,
	}
	if len(got) != len(want) {
		t.Fatalf("投递渠道数量 = %d, want %d", len(got), len(want))
	}
	for _, c := range got {
		if !want[c] {
			t.Errorf("非预期投递渠道: %s", c)
		}
	}
}

func TestChannelIsDelivery(t *testing.T) {
	if ChannelDelay.IsDelivery() || ChannelDigest.IsDelivery() {
		t.Error("delay/digest 不应为投递渠道")
	}
	if !ChannelEmail.IsDelivery() {
		t.Error("email 应为投递渠道")
	}
}

func TestParseChannel(t *testing.T) {
	if c, ok := ParseChannel("in_app"); !ok || c != ChannelInApp {
		t.Errorf("in_app 解析失败: %v %v", c, ok)
	}
	if _, ok := ParseChannel("unknown"); ok {
		t.Error("unknown 不应解析成功")
	}
}

func TestEnvironmentParse(t *testing.T) {
	if e, ok := ParseEnvironment("production"); !ok || e != EnvProduction {
		t.Errorf("production 解析失败: %v %v", e, ok)
	}
	if _, ok := ParseEnvironment("unknown"); ok {
		t.Error("unknown 不应解析成功")
	}
}
