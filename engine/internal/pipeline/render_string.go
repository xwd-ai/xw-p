package pipeline

import "fmt"

// anyToString 将任意基础类型值转为字符串（模板替换兜底）。
// 复杂结构（map/slice）仅作简单格式化，真实序列化留待 render 阶段完善。
func anyToString(v any) string {
	return fmt.Sprintf("%v", v)
}
