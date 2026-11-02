package pipeline

import (
	"fmt"
	"log/slog"

	"github.com/xwd-ai/xw-p/engine/internal/chat"
	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/mailer"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
)

// ProviderFactory 按渠道配置构造内置实现。
//
// 覆盖 console（打印日志的 mock）、inapp（写站内信收件箱）、smtp（真实提交
// 邮件）与 slack（提交群机器人的 incoming webhook）四种种类：console 用于
// 本地跑通链路，其余是真实落地的实现。twilio / fcm / apns / teams 已经在配置
// 模型里声明了凭据要求，但对应实现尚未接入，构造时返回
// provider.ErrUnsupportedKind，由装配处跳过并告警，而不是让引擎起不来。
func ProviderFactory(logger *slog.Logger) provider.Factory {
	if logger == nil {
		logger = slog.Default()
	}
	return provider.FactoryFunc(func(cfg provider.Config) (provider.Sender, error) {
		switch cfg.Kind {
		case provider.KindConsole:
			return newConsoleProvider(logger, cfg.Channel)
		case provider.KindInApp:
			if cfg.Channel != domain.ChannelInApp {
				return nil, fmt.Errorf("%w: inapp 只能用于 %s", provider.ErrUnsupportedKind, domain.ChannelInApp)
			}
			return NewInAppProvider(logger, DefaultInboxStore(logger)), nil
		case provider.KindSMTP:
			if cfg.Channel != domain.ChannelEmail {
				return nil, fmt.Errorf("%w: smtp 只能用于 %s", provider.ErrUnsupportedKind, domain.ChannelEmail)
			}
			// 凭据问题在这里落地成启动错误：host 少一段、端口非数字、明文连接
			// 带口令，都应该在装配时暴露，而不是等第一条邮件要发的时候才发现。
			mailCfg, err := mailer.ConfigFromCredentials(cfg.Credentials)
			if err != nil {
				return nil, fmt.Errorf("smtp 配置不可用（%s）：%w", cfg.ID, err)
			}
			return NewSMTPEmailProvider(logger, mailCfg)
		case provider.KindSlack:
			if cfg.Channel != domain.ChannelChat {
				return nil, fmt.Errorf("%w: slack 只能用于 %s", provider.ErrUnsupportedKind, domain.ChannelChat)
			}
			// 同样在装配期校验：webhook 少了路径、写成 http 的远程地址、
			// 表情名没带冒号，都在启动日志里指出来。
			chatCfg, err := chat.ConfigFromCredentials(cfg.Credentials)
			if err != nil {
				return nil, fmt.Errorf("slack 配置不可用（%s）：%w", cfg.ID, err)
			}
			return NewSlackChatProvider(logger, chatCfg)
		default:
			return nil, fmt.Errorf("%w: %s", provider.ErrUnsupportedKind, cfg.Kind)
		}
	})
}

// newConsoleProvider 按渠道构造 console 实现。
func newConsoleProvider(logger *slog.Logger, ch domain.Channel) (provider.Sender, error) {
	switch ch {
	case domain.ChannelInApp:
		return NewInAppProvider(logger, DefaultInboxStore(logger)), nil
	case domain.ChannelEmail:
		return NewConsoleEmailProvider(logger), nil
	case domain.ChannelSMS:
		return NewConsoleSMSProvider(logger), nil
	case domain.ChannelPush:
		return NewConsolePushProvider(logger), nil
	case domain.ChannelChat:
		return NewConsoleChatProvider(logger), nil
	default:
		return nil, fmt.Errorf("%w: console 不能投递到 %s", provider.ErrUnsupportedKind, ch)
	}
}
