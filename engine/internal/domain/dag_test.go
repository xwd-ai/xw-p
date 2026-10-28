package domain

import (
	"errors"
	"strings"
	"testing"
)

// reactionSteps 构造一组只声明顺序、不带依赖的互动触达步骤：
// 站内信 → 弹窗推送 → 邮件。
func reactionSteps() []Step {
	return []Step{
		{ID: "notify", Type: ChannelInApp, Name: "站内信"},
		{ID: "push", Type: ChannelPush, Name: "推送"},
		{ID: "mail", Type: ChannelEmail, Name: "邮件"},
	}
}

func TestValidateStepsAcceptsInteractionWorkflow(t *testing.T) {
	steps := []Step{
		{ID: "dg", Type: ChannelDigest, Name: "点赞聚合", Config: map[string]any{
			"aggregate_key": "payload.post_id",
			"window":        "1h",
		}},
		{ID: "inbox", Type: ChannelInApp, Name: "站内信", Config: map[string]any{
			"template":   "{{ payload.actor }} 赞了你的帖子",
			"depends_on": []any{"dg"},
		}},
		{ID: "push", Type: ChannelPush, Name: "推送", Config: map[string]any{
			"template":   "{{ payload.actor }} 赞了你的帖子",
			"depends_on": []string{"dg"},
		}},
	}

	if err := ValidateSteps(steps); err != nil {
		t.Fatalf("合法工作流不应报错：%v", err)
	}
}

func TestValidateStepsAllowsEmptyWorkflow(t *testing.T) {
	if err := ValidateSteps(nil); err != nil {
		t.Fatalf("空工作流应视为合法：%v", err)
	}
}

func TestValidateStepsReportsAllIDIssues(t *testing.T) {
	steps := []Step{
		{ID: "s1", Type: ChannelInApp},
		{ID: "s1", Type: ChannelPush},
		{Type: ChannelEmail},
	}

	err := ValidateSteps(steps)
	var invalid *ValidationError
	if !errors.As(err, &invalid) {
		t.Fatalf("期望 *ValidationError，实际 %v", err)
	}
	if !invalid.Has(CodeStepIDDuplicate) {
		t.Errorf("应报出重复 id：%v", invalid.Codes())
	}
	if !invalid.Has(CodeStepIDEmpty) {
		t.Errorf("应报出空 id：%v", invalid.Codes())
	}
	if len(invalid.Issues) != 2 {
		t.Errorf("应一次列全两条问题，实际 %d 条：%v", len(invalid.Issues), invalid.Issues)
	}
}

func TestValidateStepsRejectsUnknownType(t *testing.T) {
	err := ValidateSteps([]Step{{ID: "s1", Type: Channel("carrier_pigeon")}})

	var invalid *ValidationError
	if !errors.As(err, &invalid) {
		t.Fatalf("期望 *ValidationError，实际 %v", err)
	}
	if !invalid.Has(CodeStepTypeInvalid) {
		t.Fatalf("应报出类型非法：%v", invalid.Codes())
	}
	if !strings.Contains(err.Error(), "carrier_pigeon") {
		t.Errorf("错误信息应带上非法类型，实际：%v", err)
	}
}

func TestValidateStepsRejectsDanglingAndSelfDependency(t *testing.T) {
	steps := []Step{
		{ID: "s1", Type: ChannelInApp, Config: map[string]any{"depends_on": []any{"ghost"}}},
		{ID: "s2", Type: ChannelEmail, Config: map[string]any{"depends_on": []any{"s2"}}},
	}

	err := ValidateSteps(steps)
	var invalid *ValidationError
	if !errors.As(err, &invalid) {
		t.Fatalf("期望 *ValidationError，实际 %v", err)
	}
	if !invalid.Has(CodeDependsOnUnknown) {
		t.Errorf("应报出悬空依赖：%v", invalid.Codes())
	}
	if !invalid.Has(CodeDependsOnSelf) {
		t.Errorf("应报出自依赖：%v", invalid.Codes())
	}
}

func TestValidateStepsRejectsMalformedDependsOn(t *testing.T) {
	cases := []struct {
		name string
		raw  any
	}{
		{name: "字符串而非数组", raw: "s1"},
		{name: "数组内含非字符串", raw: []any{"s1", 42}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSteps([]Step{{
				ID:     "s1",
				Type:   ChannelInApp,
				Config: map[string]any{"depends_on": tc.raw},
			}})

			var invalid *ValidationError
			if !errors.As(err, &invalid) {
				t.Fatalf("期望 *ValidationError，实际 %v", err)
			}
			if !invalid.Has(CodeDependsOnInvalid) {
				t.Fatalf("应报出 depends_on 格式错误：%v", invalid.Codes())
			}
		})
	}
}

func TestValidateStepsDetectsCycle(t *testing.T) {
	steps := []Step{
		{ID: "s1", Type: ChannelInApp, Config: map[string]any{"depends_on": []any{"s3"}}},
		{ID: "s2", Type: ChannelPush, Config: map[string]any{"depends_on": []any{"s1"}}},
		{ID: "s3", Type: ChannelEmail, Config: map[string]any{"depends_on": []any{"s2"}}},
	}

	err := ValidateSteps(steps)
	var invalid *ValidationError
	if !errors.As(err, &invalid) {
		t.Fatalf("期望 *ValidationError，实际 %v", err)
	}
	if !invalid.Has(CodeCycle) {
		t.Fatalf("应报出依赖环：%v", invalid.Codes())
	}
	// 环路径带方向，便于直接定位是哪几个步骤互相依赖。
	if !strings.Contains(err.Error(), "→") {
		t.Errorf("错误信息应包含环路径，实际：%v", err)
	}
	if got := len(invalid.Issues); got != 1 {
		t.Errorf("应只报一条环问题，实际 %d：%v", got, invalid.Issues)
	}
}

func TestValidateStepsDetectsSelfLoopAsSelfDependency(t *testing.T) {
	// 自依赖报成「依赖自身」而非「环」，错误信息更直白。
	err := ValidateSteps([]Step{{
		ID:     "only",
		Type:   ChannelSMS,
		Config: map[string]any{"depends_on": []any{"only"}},
	}})

	var invalid *ValidationError
	if !errors.As(err, &invalid) {
		t.Fatalf("期望 *ValidationError，实际 %v", err)
	}
	if invalid.Has(CodeCycle) {
		t.Errorf("自依赖不应同时报成环：%v", invalid.Codes())
	}
	if !invalid.Has(CodeDependsOnSelf) {
		t.Errorf("应报出自依赖：%v", invalid.Codes())
	}
}

func TestDependsOnParsesBothShapes(t *testing.T) {
	fromJSON := Step{ID: "s1", Config: map[string]any{"depends_on": []any{"a", " b "}}}
	deps, err := DependsOn(fromJSON)
	if err != nil {
		t.Fatalf("解析 JSON 形态的依赖失败：%v", err)
	}
	if len(deps) != 2 || deps[0] != "a" || deps[1] != "b" {
		t.Fatalf("依赖解析结果不符：%v", deps)
	}

	none, err := DependsOn(Step{ID: "s2"})
	if err != nil || none != nil {
		t.Fatalf("未声明依赖时应返回 nil, nil，实际 %v, %v", none, err)
	}
}

func TestOrderStepsRespectsDependencies(t *testing.T) {
	// 声明顺序与依赖顺序相反：邮件依赖推送，推送依赖站内信。
	steps := []Step{
		{ID: "mail", Type: ChannelEmail, Config: map[string]any{"depends_on": []any{"push"}}},
		{ID: "push", Type: ChannelPush, Config: map[string]any{"depends_on": []string{"inbox"}}},
		{ID: "inbox", Type: ChannelInApp},
	}

	ordered, err := OrderSteps(steps)
	if err != nil {
		t.Fatalf("排序失败：%v", err)
	}

	got := make([]string, 0, len(ordered))
	for _, step := range ordered {
		got = append(got, step.ID)
	}
	want := []string{"inbox", "push", "mail"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("依赖顺序不符，期望 %v，实际 %v", want, got)
		}
	}
}

func TestOrderStepsKeepsOriginalOrderWithoutDependencies(t *testing.T) {
	steps := reactionSteps()

	ordered, err := OrderSteps(steps)
	if err != nil {
		t.Fatalf("排序失败：%v", err)
	}
	for i, step := range ordered {
		if step.ID != steps[i].ID {
			t.Fatalf("无依赖时应保持原序，第 %d 项期望 %s，实际 %s", i, steps[i].ID, step.ID)
		}
	}
}

func TestOrderStepsStableAcrossIndependentBranches(t *testing.T) {
	// 两个互不相关的分支：同一轮就绪的节点按原始下标出队，
	// 因此「刚解锁的低下标节点」会排在原序更靠后的独立步骤之前。
	steps := []Step{
		{ID: "a1", Type: ChannelInApp},
		{ID: "b1", Type: ChannelPush, Config: map[string]any{"depends_on": []any{"a1"}}},
		{ID: "a2", Type: ChannelEmail},
		{ID: "b2", Type: ChannelSMS, Config: map[string]any{"depends_on": []any{"b1"}}},
	}

	ordered, err := OrderSteps(steps)
	if err != nil {
		t.Fatalf("排序失败：%v", err)
	}
	want := []string{"a1", "b1", "a2", "b2"} // 每轮都取原始下标最小者：a1 → b1 → a2 → b2
	for i, step := range ordered {
		if step.ID != want[i] {
			t.Fatalf("稳定排序结果不符，期望 %v，实际第 %d 项为 %s", want, i, step.ID)
		}
	}
}

func TestOrderStepsPropagatesValidationError(t *testing.T) {
	if _, err := OrderSteps([]Step{
		{ID: "s1", Type: ChannelInApp, Config: map[string]any{"depends_on": []any{"ghost"}}},
	}); err == nil {
		t.Fatal("非法依赖应在排序前被拒绝")
	}
}
