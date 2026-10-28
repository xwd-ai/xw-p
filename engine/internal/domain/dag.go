package domain

import (
	"fmt"
	"sort"
	"strings"
)

// ValidationCode 是工作流校验问题的机器可读分类，便于调用方按类别处理
// （例如前端高亮某个步骤、接口返回结构化错误）。
type ValidationCode string

const (
	// CodeStepIDEmpty 步骤缺少 id。
	CodeStepIDEmpty ValidationCode = "step_id_empty"
	// CodeStepIDDuplicate 同一步骤 id 出现多次。
	CodeStepIDDuplicate ValidationCode = "step_id_duplicate"
	// CodeStepTypeInvalid 步骤类型不在 StepType 枚举内。
	CodeStepTypeInvalid ValidationCode = "step_type_invalid"
	// CodeDependsOnInvalid depends_on 不是字符串数组。
	CodeDependsOnInvalid ValidationCode = "depends_on_invalid"
	// CodeDependsOnUnknown depends_on 引用了不存在的步骤 id。
	CodeDependsOnUnknown ValidationCode = "depends_on_unknown"
	// CodeDependsOnSelf 步骤依赖自身。
	CodeDependsOnSelf ValidationCode = "depends_on_self"
	// CodeCycle 依赖关系存在环。
	CodeCycle ValidationCode = "cycle"
)

// ValidationIssue 描述一条具体的校验问题。
type ValidationIssue struct {
	Code    ValidationCode
	StepID  string
	Message string
}

// Error 实现 error，输出形如「步骤 s2：依赖了不存在的步骤 ghost」。
func (i ValidationIssue) Error() string {
	if i.StepID == "" {
		return i.Message
	}
	return fmt.Sprintf("步骤 %s：%s", i.StepID, i.Message)
}

// ValidationError 汇总一次校验发现的全部问题。
//
// 校验不做「首个错误即返回」：一次把问题列全，工作流编辑器可以一次改到位。
type ValidationError struct {
	Issues []ValidationIssue
}

// Error 实现 error，多条问题以分号连接。
func (e *ValidationError) Error() string {
	if e == nil || len(e.Issues) == 0 {
		return "工作流校验失败"
	}
	parts := make([]string, 0, len(e.Issues))
	for _, issue := range e.Issues {
		parts = append(parts, issue.Error())
	}
	return "工作流校验失败：" + strings.Join(parts, "；")
}

// Codes 返回去重后的问题分类，顺序与首次出现顺序一致。
func (e *ValidationError) Codes() []ValidationCode {
	if e == nil {
		return nil
	}
	seen := make(map[ValidationCode]struct{}, len(e.Issues))
	codes := make([]ValidationCode, 0, len(e.Issues))
	for _, issue := range e.Issues {
		if _, ok := seen[issue.Code]; ok {
			continue
		}
		seen[issue.Code] = struct{}{}
		codes = append(codes, issue.Code)
	}
	return codes
}

// Has 判断是否包含指定分类的问题。
func (e *ValidationError) Has(code ValidationCode) bool {
	if e == nil {
		return false
	}
	for _, issue := range e.Issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

// DependsOn 解析步骤 config 中的 depends_on 声明。
//
// 支持两种来源：手写代码里的 []string，以及 JSON 反序列化得到的 []any。
// 未声明依赖时返回 nil, nil（区别于「声明了但格式错」的报错）。
func DependsOn(step Step) ([]string, error) {
	raw, ok := step.Config["depends_on"]
	if !ok || raw == nil {
		return nil, nil
	}

	switch value := raw.(type) {
	case []string:
		out := make([]string, 0, len(value))
		for _, dep := range value {
			out = append(out, strings.TrimSpace(dep))
		}
		return out, nil
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			dep, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("depends_on 必须是字符串数组，其中含 %T", item)
			}
			out = append(out, strings.TrimSpace(dep))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("depends_on 必须是字符串数组，实际为 %T", raw)
	}
}

// ValidateSteps 校验步骤序列：id 非空且唯一、类型合法、依赖引用存在、
// 依赖关系无环。空步骤列表视为合法的空工作流。
//
// 返回 *ValidationError 时可用 errors.As 取出全部问题。
func ValidateSteps(steps []Step) error {
	var issues []ValidationIssue

	// 1) 单步骤：id 非空且唯一、类型合法。
	index := make(map[string]int, len(steps))
	for _, step := range steps {
		id := strings.TrimSpace(step.ID)
		if id == "" {
			issues = append(issues, ValidationIssue{
				Code:    CodeStepIDEmpty,
				Message: "步骤缺少 id，依赖关系无法表达",
			})
		} else if _, dup := index[id]; dup {
			issues = append(issues, ValidationIssue{
				Code:    CodeStepIDDuplicate,
				StepID:  id,
				Message: "步骤 id 重复",
			})
		} else {
			index[id] = len(index)
		}

		if _, ok := ParseStepType(string(step.Type)); !ok {
			issues = append(issues, ValidationIssue{
				Code:    CodeStepTypeInvalid,
				StepID:  id,
				Message: fmt.Sprintf("类型 %q 不在 %v 之内", string(step.Type), AllStepTypes()),
			})
		}
	}

	// 2) 依赖声明：格式、引用存在性、自依赖。
	for _, step := range steps {
		deps, err := DependsOn(step)
		if err != nil {
			issues = append(issues, ValidationIssue{
				Code:    CodeDependsOnInvalid,
				StepID:  strings.TrimSpace(step.ID),
				Message: err.Error(),
			})
			continue
		}
		for _, dep := range deps {
			if dep == strings.TrimSpace(step.ID) {
				issues = append(issues, ValidationIssue{
					Code:    CodeDependsOnSelf,
					StepID:  dep,
					Message: "步骤不能依赖自身",
				})
				continue
			}
			if _, ok := index[dep]; !ok {
				issues = append(issues, ValidationIssue{
					Code:    CodeDependsOnUnknown,
					StepID:  strings.TrimSpace(step.ID),
					Message: fmt.Sprintf("依赖了不存在的步骤 %q", dep),
				})
			}
		}
	}

	// 3) 环检测：前面已发现图层面的问题（重复 id、悬空依赖），
	// 此时依赖图不完整，跳过环检测以免报出误导性的环。
	if len(issues) == 0 {
		if cycle := findCycle(steps); len(cycle) > 0 {
			issues = append(issues, ValidationIssue{
				Code:    CodeCycle,
				StepID:  cycle[0],
				Message: "依赖存在环：" + strings.Join(cycle, " → "),
			})
		}
	}

	if len(issues) == 0 {
		return nil
	}
	return &ValidationError{Issues: issues}
}

// OrderSteps 校验步骤依赖并返回按依赖先后排列的步骤序列。
//
// 排序是稳定的：无依赖约束的步骤保持原有顺序，因此「只声明少量依赖」的
// 工作流执行顺序与编排器中看到的顺序一致。校验失败时返回 *ValidationError。
func OrderSteps(steps []Step) ([]Step, error) {
	if err := ValidateSteps(steps); err != nil {
		return nil, err
	}
	if len(steps) == 0 {
		return nil, nil
	}
	order := topoOrder(steps)
	out := make([]Step, 0, len(steps))
	for _, i := range order {
		out = append(out, steps[i])
	}
	return out, nil
}

// topoOrder 返回拓扑序的原始下标序列；调用前需保证依赖图合法。
//
// 同一轮就绪的节点按原始下标排序，保证相同输入得到相同输出。
func topoOrder(steps []Step) []int {
	index := make(map[string]int, len(steps))
	for i, step := range steps {
		index[step.ID] = i
	}

	indegree := make([]int, len(steps))
	dependents := make([][]int, len(steps))
	for i, step := range steps {
		deps, err := DependsOn(step)
		if err != nil {
			continue
		}
		for _, dep := range deps {
			j, ok := index[dep]
			if !ok || j == i {
				continue
			}
			indegree[i]++
			dependents[j] = append(dependents[j], i)
		}
	}

	ready := make([]int, 0, len(steps))
	for i, deg := range indegree {
		if deg == 0 {
			ready = append(ready, i)
		}
	}

	order := make([]int, 0, len(steps))
	for len(ready) > 0 {
		next := ready[0]
		ready = ready[1:]
		order = append(order, next)
		for _, child := range dependents[next] {
			indegree[child]--
			if indegree[child] == 0 {
				ready = insertAscending(ready, child)
			}
		}
	}
	return order
}

// insertAscending 把下标插入已升序的切片，保持升序。
func insertAscending(sorted []int, value int) []int {
	pos := sort.SearchInts(sorted, value)
	sorted = append(sorted, 0)
	copy(sorted[pos+1:], sorted[pos:])
	sorted[pos] = value
	return sorted
}

// findCycle 返回一条具体的依赖环（首尾相同，如 [s1 s2 s1]）；无环返回 nil。
//
// 与拓扑排序互补：拓扑排序只回答「有没有环」，这里回答「环长什么样」，
// 报错信息可以直接指给用户看是哪几个步骤互相依赖。
func findCycle(steps []Step) []string {
	index := make(map[string]int, len(steps))
	for i, step := range steps {
		index[step.ID] = i
	}

	const (
		unvisited uint8 = iota
		onStack
		done
	)
	state := make([]uint8, len(steps))
	stack := make([]int, 0, len(steps))

	var dfs func(i int) []string
	dfs = func(i int) []string {
		state[i] = onStack
		stack = append(stack, i)

		deps, err := DependsOn(steps[i])
		if err == nil {
			for _, dep := range deps {
				j, ok := index[dep]
				if !ok || j == i {
					continue
				}
				switch state[j] {
				case onStack:
					start := 0
					for k, node := range stack {
						if node == j {
							start = k
							break
						}
					}
					path := make([]string, 0, len(stack)-start+1)
					for _, node := range stack[start:] {
						path = append(path, steps[node].ID)
					}
					return append(path, steps[j].ID)
				case unvisited:
					if cycle := dfs(j); cycle != nil {
						return cycle
					}
				}
			}
		}

		stack = stack[:len(stack)-1]
		state[i] = done
		return nil
	}

	for i := range steps {
		if state[i] != unvisited {
			continue
		}
		if cycle := dfs(i); cycle != nil {
			return cycle
		}
	}
	return nil
}
