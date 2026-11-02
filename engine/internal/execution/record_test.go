package execution

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func strPtr(s string) *string { return &s }

// 时间戳必须按「六位小数 + 显式偏移」输出：API 侧用 datetime.fromisoformat
// 解析，Z 后缀在旧版本上不认，纳秒尾数也会被截断。
func TestTimestampMarshalUsesMicrosecondsAndOffset(t *testing.T) {
	ts := NewTimestamp(time.Date(2026, 11, 2, 10, 9, 12, 123456789, time.UTC))

	data, err := json.Marshal(ts)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	if got, want := string(data), `"2026-11-02T10:09:12.123456+00:00"`; got != want {
		t.Errorf("时间戳格式不符：得到 %s，期望 %s", got, want)
	}
}

func TestTimestampMarshalsZeroAsNull(t *testing.T) {
	data, err := json.Marshal(Timestamp{})
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	if string(data) != "null" {
		t.Errorf("零值时间应序列化为 null，实际 %s", data)
	}
}

func TestTimestampParsesOffsets(t *testing.T) {
	cases := map[string]time.Time{
		"2026-11-02T10:09:12.123456+00:00": time.Date(2026, 11, 2, 10, 9, 12, 123456000, time.UTC),
		"2026-11-02T18:09:12+08:00":        time.Date(2026, 11, 2, 10, 9, 12, 0, time.UTC),
		"2026-11-02T10:09:12Z":             time.Date(2026, 11, 2, 10, 9, 12, 0, time.UTC),
	}
	for raw, want := range cases {
		var ts Timestamp
		if err := json.Unmarshal([]byte(`"`+raw+`"`), &ts); err != nil {
			t.Fatalf("解析 %s 失败: %v", raw, err)
		}
		if !ts.UTC().Equal(want) {
			t.Errorf("解析 %s 得到 %s，期望 %s", raw, ts.UTC(), want)
		}
	}
}

func TestTimestampRejectsGarbage(t *testing.T) {
	var ts Timestamp
	if err := json.Unmarshal([]byte(`"昨天"`), &ts); err == nil {
		t.Fatalf("非 ISO 8601 字符串应报错")
	}
}

func TestRecordValidate(t *testing.T) {
	base := Record{
		TriggerID: "t-1",
		Channel:   "email",
		Status:    StatusSent,
	}

	cases := []struct {
		name    string
		mutate  func(Record) Record
		wantErr bool
		mention string
	}{
		{"成功记录", func(r Record) Record { return r }, false, ""},
		{"失败记录带原因", func(r Record) Record {
			r.Status = StatusFailed
			r.Reason = strPtr("SMTP 550")
			return r
		}, false, ""},
		{"缺触发标识", func(r Record) Record { r.TriggerID = "  "; return r }, true, "trigger_id"},
		{"缺渠道", func(r Record) Record { r.Channel = ""; return r }, true, "channel"},
		{"未知状态", func(r Record) Record { r.Status = "queued"; return r }, true, "status"},
		{"失败但没原因", func(r Record) Record { r.Status = StatusFailed; return r }, true, "reason"},
		{"失败原因全是空白", func(r Record) Record {
			r.Status = StatusFailed
			r.Reason = strPtr("   ")
			return r
		}, true, "reason"},
		{"耗时不能为负", func(r Record) Record { r.DurationMS = -1; return r }, true, "duration_ms"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.mutate(base).Validate()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("期望校验失败")
				}
				if !errors.Is(err, ErrInvalidRecord) {
					t.Errorf("错误应包装 ErrInvalidRecord: %v", err)
				}
				if !strings.Contains(err.Error(), tc.mention) {
					t.Errorf("错误信息应提到 %s: %v", tc.mention, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("期望校验通过，实际 %v", err)
			}
		})
	}
}

// 落库的键名必须与 API 侧的读取实现一致：多出来的排查字段可以为空，
// 但 trigger_id / channel / status / reason / duration_ms / created_at 不能改名。
func TestRecordJSONShape(t *testing.T) {
	sent := Record{
		ID:         "abc",
		TriggerID:  "t-1",
		Channel:    "email",
		Status:     StatusSent,
		DurationMS: 12,
		CreatedAt:  NewTimestamp(time.Date(2026, 11, 2, 10, 9, 12, 0, time.UTC)),
	}
	data, err := json.Marshal(sent)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}

	for _, key := range []string{"id", "trigger_id", "channel", "status", "reason", "duration_ms", "created_at"} {
		if _, ok := fields[key]; !ok {
			t.Errorf("缺少字段 %s：%s", key, data)
		}
	}
	if fields["reason"] != nil {
		t.Errorf("成功记录的 reason 应为 null，实际 %v", fields["reason"])
	}
	// 空值字段按 omitempty 省略，避免每行都塞一堆空串。
	for _, key := range []string{"workflow", "subscriber_id", "step_id", "provider", "attempts", "fallback"} {
		if _, ok := fields[key]; ok {
			t.Errorf("空值字段 %s 不应落库：%s", key, data)
		}
	}

	failed := Record{
		TriggerID: "t-2",
		Workflow:  "post_commented",
		StepID:    "notify",
		Channel:   "email",
		Status:    StatusFailed,
		Reason:    strPtr("SMTP 550"),
		Provider:  "console-email",
		Attempts:  []string{"smtp-email", "console-email"},
		Fallback:  true,
		CreatedAt: NewTimestamp(time.Now()),
	}
	data, err = json.Marshal(failed)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	if !strings.Contains(string(data), `"attempts":["smtp-email","console-email"]`) {
		t.Errorf("尝试序列未按顺序落库：%s", data)
	}
	if !strings.Contains(string(data), `"fallback":true`) {
		t.Errorf("回退标记未落库：%s", data)
	}
	if !strings.Contains(string(data), `"reason":"SMTP 550"`) {
		t.Errorf("失败原因未落库：%s", data)
	}
}
