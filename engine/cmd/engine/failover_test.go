package main

import (
	"strings"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
)

// 启动日志里的「渠道取用序列」应当只包含有备选的渠道，且按优先级排好：
// 第一个是默认实现，其后是失败时依次回退的备选。
func TestFallbackChainsListsStandbyOrder(t *testing.T) {
	reg := pipeline.NewProviderRegistry()
	register := func(id string, ch domain.Channel, priority int, enabled bool) {
		t.Helper()
		err := reg.RegisterEntry(provider.Entry{
			ID:       id,
			Channel:  ch,
			Priority: priority,
			Enabled:  enabled,
			Sender:   pipeline.NewConsoleEmailProvider(discardLogger()),
		})
		if err != nil {
			// console email 只声明 email 渠道，其余渠道要换实现，这里只测 email。
			t.Fatalf("登记 %s 失败：%v", id, err)
		}
	}

	register("console-email", domain.ChannelEmail, 100, true)
	register("smtp-email", domain.ChannelEmail, 10, true)
	register("legacy-email", domain.ChannelEmail, 5, false)

	chains := fallbackChains(reg)
	got, ok := chains[string(domain.ChannelEmail)]
	if !ok {
		t.Fatalf("email 有多条启用实现，应出现在取用序列里：%+v", chains)
	}
	if want := "smtp-email,console-email"; strings.Join(got, ",") != want {
		t.Errorf("email 取用序列 = %v, 期望 %s（停用条目被排除）", got, want)
	}
	if len(chains) != 1 {
		t.Errorf("只有 email 有备选，其余渠道不应出现：%+v", chains)
	}
}

// 单实现渠道没有主备关系，启动日志不该为它打印一条空序列。
func TestFallbackChainsEmptyForSingleProvider(t *testing.T) {
	reg := pipeline.DefaultProviderRegistry(discardLogger())
	if chains := fallbackChains(reg); len(chains) != 0 {
		t.Errorf("默认装配每个渠道只有一个实现，不应产生取用序列：%+v", chains)
	}
}
