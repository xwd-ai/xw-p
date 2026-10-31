package pipeline

import (
	"fmt"
	"log/slog"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
)

// ProviderFactory 按渠道配置构造内置实现。
//
// 目前覆盖 console（打印日志的 mock）与 inapp（写站内信收件箱）两种种类：
// 前者用于本地跑通链路，后者是真实落库的实现。smtp / twilio / fcm / apns /
// slack / teams 已经在配置模型里声明了凭据要求，但对应实现尚未接入，构造时
// 返回 provider.ErrUnsupportedKind，由装配处跳过并告警，而不是让引擎起不来。
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
