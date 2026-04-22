mod agent;
mod agent_robustness;
mod backup_cron_scheduling;
mod hooks;
mod memory_comparison;
mod memory_loop_continuity;
mod memory_restart;
mod report_template_tool_test;

// Channel-specific integration tests — require agent-runtime (full channels)
#[cfg(feature = "agent-runtime")]
mod channel_matrix;
#[cfg(feature = "agent-runtime")]
mod channel_routing;
#[cfg(feature = "agent-runtime")]
mod email_attachments;
#[cfg(feature = "agent-runtime")]
mod telegram_attachment_fallback;
#[cfg(feature = "agent-runtime")]
mod telegram_finalize_draft;
