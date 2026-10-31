package pipeline

import (
	"fmt"
	"log/slog"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/mailer"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
)

// ProviderFactory 按渠道配置构造内置实现。
//
// 覆盖 console（打印日志的 mock）、inapp（写站内信收件箱）与 smtp（真实提交
// 邮件）三种种类：console 用于本地跑通链路，inapp 与 smtp 是真实落地的实现。
// twilio / fcm / apns / slack / teams 已经在配置模型里声明了凭据要求，但对应
// 实现尚未接入，构造时返回 provider.ErrUnsupportedKind，由装配处跳过并告警，
// 而不是让引擎起不来。
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
