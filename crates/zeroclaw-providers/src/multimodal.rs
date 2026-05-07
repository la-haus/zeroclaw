use base64::{Engine as _, engine::general_purpose::STANDARD};
use regex::Regex;
use reqwest::Client;
use std::path::Path;
use std::sync::LazyLock;
use zeroclaw_api::provider::ChatMessage;
use zeroclaw_config::schema::{MultimodalConfig, build_runtime_proxy_client_with_timeouts};

const IMAGE_MARKER_PREFIX: &str = "[IMAGE:";
const DOCUMENT_MARKER_PREFIX: &str = "[DOCUMENT:";

struct SkippedAttachment {
    url: String,
    kind: &'static str,
    reason: String,
}
const ALLOWED_IMAGE_MIME_TYPES: &[&str] = &[
    "image/png",
    "image/jpeg",
    "image/webp",
    "image/gif",
    "image/bmp",
];
const ALLOWED_DOCUMENT_MIME_TYPES: &[&str] = &["application/pdf"];

/// MIME types that should be inlined as text in the message (not as document blocks).
const TEXT_DOCUMENT_MIME_TYPES: &[&str] = &["text/plain", "text/csv"];

/// MIME types that can be converted to PDF via office2pdf before sending.
#[cfg(feature = "office-convert")]
const CONVERTIBLE_OFFICE_MIME_TYPES: &[(&str, office2pdf::config::Format)] = &[
    (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        office2pdf::config::Format::Docx,
    ),
    (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        office2pdf::config::Format::Xlsx,
    ),
    (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        office2pdf::config::Format::Pptx,
    ),
];

/// Office MIME types (used for detection even when conversion is disabled).
#[cfg(not(feature = "office-convert"))]
const CONVERTIBLE_OFFICE_MIME_TYPES: &[&str] = &[
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
];

/// Maximum document size in bytes (32 MB — Anthropic PDF limit).
const MAX_DOCUMENT_BYTES: usize = 32 * 1024 * 1024;

/// Image file extensions that trigger `[IMAGE:url]` markers when detected in URLs.
const IMAGE_EXTENSIONS: &[&str] = &["png", "jpg", "jpeg", "webp", "gif", "bmp"];

/// Document file extensions that trigger `[DOCUMENT:url]` markers when detected in URLs.
const DOCUMENT_EXTENSIONS: &[&str] = &["pdf", "csv", "txt", "docx", "xlsx", "pptx"];

/// File extensions recognized but NOT supported — detected for descriptive skip messages.
const UNSUPPORTED_IMAGE_EXTENSIONS: &[&str] = &["heic", "heif"];

/// Regex matching URLs (http/https) ending in a supported file extension.
/// Captures: full URL including optional query string, and the extension.
static URL_WITH_EXTENSION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(https?://[^\s\]\)>]+\.(?:png|jpe?g|webp|gif|bmp|pdf|csv|txt|docx|xlsx|pptx|heic|heif))(?:\?[^\s\]\)>]*)?"
    )
    .expect("URL_WITH_EXTENSION_RE must compile")
});

#[derive(Debug, Clone)]
pub struct PreparedMessages {
    pub messages: Vec<ChatMessage>,
    pub contains_images: bool,
    pub contains_documents: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum MultimodalError {
    #[error("multimodal image limit exceeded: max_images={max_images}, found={found}")]
    TooManyImages { max_images: usize, found: usize },

    #[error(
        "multimodal image size limit exceeded for '{input}': {size_bytes} bytes > {max_bytes} bytes"
    )]
    ImageTooLarge {
        input: String,
        size_bytes: usize,
        max_bytes: usize,
    },

    #[error("multimodal image MIME type is not allowed for '{input}': {mime}")]
    UnsupportedMime { input: String, mime: String },

    #[error("multimodal remote image fetch is disabled for '{input}'")]
    RemoteFetchDisabled { input: String },

    #[error("multimodal image source not found or unreadable: '{input}'")]
    ImageSourceNotFound { input: String },

    #[error("invalid multimodal image marker '{input}': {reason}")]
    InvalidMarker { input: String, reason: String },

    #[error("failed to download remote image '{input}': {reason}")]
    RemoteFetchFailed { input: String, reason: String },

    #[error("failed to read local image '{input}': {reason}")]
    LocalReadFailed { input: String, reason: String },
}

/// Scan user message text for URLs with recognized file extensions and inject
/// `[IMAGE:url]` or `[DOCUMENT:url]` markers so the multimodal pipeline can
/// process them. URLs that already appear inside existing markers are skipped.
/// Unsupported formats (HEIC/HEIF) are flagged with a descriptive placeholder.
pub fn inject_url_markers(content: &str) -> String {
    // Collect (end_offset, marker_to_insert) pairs using actual regex match positions.
    let mut insertions: Vec<(usize, String)> = Vec::new();

    for m in URL_WITH_EXTENSION_RE.find_iter(content) {
        let url = m.as_str();
        let pos = m.start();

        // Use the actual match position (not content.find) to check context
        let before = &content[..pos];
        let last_open_image = before.rfind(IMAGE_MARKER_PREFIX);
        let last_open_doc = before.rfind(DOCUMENT_MARKER_PREFIX);
        let last_close = before.rfind(']');

        let inside_marker = match (last_open_image.or(last_open_doc), last_close) {
            (Some(open), Some(close)) => open > close,
            (Some(_), None) => true,
            _ => false,
        };
        if inside_marker {
            continue;
        }

        // Check if a marker already follows this URL
        let after = &content[m.end()..];
        if after.starts_with(" [IMAGE:") || after.starts_with(" [DOCUMENT:") {
            continue;
        }

        // Extract extension from the URL path (before query string)
        let url_path = url.split('?').next().unwrap_or(url);
        let ext = url_path
            .rsplit('.')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();

        let marker = if IMAGE_EXTENSIONS.contains(&ext.as_str()) {
            format!(" [IMAGE:{url}]")
        } else if DOCUMENT_EXTENSIONS.contains(&ext.as_str()) {
            format!(" [DOCUMENT:{url}]")
        } else if UNSUPPORTED_IMAGE_EXTENSIONS.contains(&ext.as_str()) {
            format!(
                " [Unsupported file: {url} — HEIC/HEIF format is not supported by the AI provider. Ask the user to resend as JPEG or PNG.]"
            )
        } else {
            continue;
        };

        tracing::debug!(
            url,
            marker_kind = ext.as_str(),
            "Injecting URL marker from plain text"
        );
        insertions.push((m.end(), marker));
    }

    if insertions.is_empty() {
        return content.to_string();
    }

    // Build result by inserting markers at correct positions (process right-to-left
    // to preserve offsets when inserting into the string).
    let mut result = content.to_string();
    for (end_pos, marker) in insertions.into_iter().rev() {
        result.insert_str(end_pos, &marker);
    }

    result
}

pub fn parse_image_markers(content: &str) -> (String, Vec<String>) {
    let mut refs = Vec::new();
    let mut cleaned = String::with_capacity(content.len());
    let mut cursor = 0usize;

    while let Some(rel_start) = content[cursor..].find(IMAGE_MARKER_PREFIX) {
        let start = cursor + rel_start;
        cleaned.push_str(&content[cursor..start]);

        let marker_start = start + IMAGE_MARKER_PREFIX.len();
        let Some(rel_end) = content[marker_start..].find(']') else {
            cleaned.push_str(&content[start..]);
            cursor = content.len();
            break;
        };

        let end = marker_start + rel_end;
        let candidate = content[marker_start..end].trim();

        if candidate.is_empty() {
            cleaned.push_str(&content[start..=end]);
        } else {
            refs.push(candidate.to_string());
        }

        cursor = end + 1;
    }

    if cursor < content.len() {
        cleaned.push_str(&content[cursor..]);
    }

    (cleaned.trim().to_string(), refs)
}

pub fn count_image_markers(messages: &[ChatMessage]) -> usize {
    messages
        .iter()
        .filter(|m| m.role == "user")
        .map(|m| parse_image_markers(&m.content).1.len())
        .sum()
}

pub fn contains_image_markers(messages: &[ChatMessage]) -> bool {
    count_image_markers(messages) > 0
}

pub fn parse_document_markers(content: &str) -> (String, Vec<String>) {
    let mut refs = Vec::new();
    let mut cleaned = String::with_capacity(content.len());
    let mut cursor = 0usize;

    while let Some(rel_start) = content[cursor..].find(DOCUMENT_MARKER_PREFIX) {
        let start = cursor + rel_start;
        cleaned.push_str(&content[cursor..start]);

        let marker_start = start + DOCUMENT_MARKER_PREFIX.len();
        let Some(rel_end) = content[marker_start..].find(']') else {
            cleaned.push_str(&content[start..]);
            cursor = content.len();
            break;
        };

        let end = marker_start + rel_end;
        let candidate = content[marker_start..end].trim();

        if candidate.is_empty() {
            cleaned.push_str(&content[start..=end]);
        } else {
            refs.push(candidate.to_string());
        }

        cursor = end + 1;
    }

    if cursor < content.len() {
        cleaned.push_str(&content[cursor..]);
    }

    (cleaned.trim().to_string(), refs)
}

pub fn count_document_markers(messages: &[ChatMessage]) -> usize {
    messages
        .iter()
        .filter(|m| m.role == "user")
        .map(|m| parse_document_markers(&m.content).1.len())
        .sum()
}

pub fn contains_document_markers(messages: &[ChatMessage]) -> bool {
    count_document_markers(messages) > 0
}

pub fn extract_ollama_image_payload(image_ref: &str) -> Option<String> {
    if image_ref.starts_with("data:") {
        let comma_idx = image_ref.find(',')?;
        let (_, payload) = image_ref.split_at(comma_idx + 1);
        let payload = payload.trim();
        if payload.is_empty() {
            None
        } else {
            Some(payload.to_string())
        }
    } else {
        Some(image_ref.trim().to_string()).filter(|value| !value.is_empty())
    }
}

pub async fn prepare_messages_for_provider(
    messages: &[ChatMessage],
    config: &MultimodalConfig,
) -> anyhow::Result<PreparedMessages> {
    let (max_images, max_image_size_mb) = config.effective_limits();
    let max_bytes = max_image_size_mb.saturating_mul(1024 * 1024);

    // Pre-process: detect URLs with supported file extensions in user messages
    // and inject [IMAGE:url] or [DOCUMENT:url] markers automatically.
    let enriched: Vec<ChatMessage> = messages
        .iter()
        .map(|m| {
            if m.role == "user" && (m.content.contains("http://") || m.content.contains("https://"))
            {
                let injected = inject_url_markers(&m.content);
                if injected != m.content {
                    tracing::debug!(
                        original_len = m.content.len(),
                        enriched_len = injected.len(),
                        "Injected URL markers from plain text"
                    );
                    return ChatMessage {
                        role: m.role.clone(),
                        content: injected,
                    };
                }
            }
            m.clone()
        })
        .collect();

    let total_images = count_image_markers(&enriched);
    let total_documents = count_document_markers(&enriched);

    if total_images == 0 && total_documents == 0 {
        return Ok(PreparedMessages {
            messages: enriched,
            contains_images: false,
            contains_documents: false,
        });
    }

    // When image count exceeds the limit, strip markers from oldest messages
    // first so that the most recent (most relevant) images survive. This
    // prevents conversations from becoming permanently stuck once the
    // cumulative image count crosses the threshold.
    let trimmed = if total_images > max_images {
        trim_old_images(&enriched, max_images)
    } else {
        enriched
    };

    let remote_client = build_runtime_proxy_client_with_timeouts("provider.ollama", 30, 10);

    let mut normalized_messages = Vec::with_capacity(trimmed.len());
    let mut has_images = false;
    let mut has_documents = false;

    for message in &trimmed {
        if message.role != "user" {
            normalized_messages.push(message.clone());
            continue;
        }

        // --- Image processing (non-fatal) ---
        let (text_after_images, image_refs) = parse_image_markers(&message.content);

        let mut normalized_image_refs = Vec::with_capacity(image_refs.len());
        let mut skipped_attachments: Vec<SkippedAttachment> = Vec::new();
        for reference in &image_refs {
            match normalize_image_reference(reference, config, max_bytes, &remote_client).await {
                Ok(data_uri) => normalized_image_refs.push(data_uri),
                Err(e) => {
                    tracing::warn!(source = %reference, error = %e, "Skipping invalid image in multimodal pipeline");
                    skipped_attachments.push(SkippedAttachment {
                        url: reference.clone(),
                        kind: "image",
                        reason: e.to_string(),
                    });
                }
            }
        }

        // --- Document processing (non-fatal) ---
        let (text_after_docs, doc_refs) = parse_document_markers(&text_after_images);

        let mut normalized_doc_refs = Vec::with_capacity(doc_refs.len());
        for reference in &doc_refs {
            match normalize_remote_document(reference, config, &remote_client).await {
                Ok(data_uri) => normalized_doc_refs.push(data_uri),
                Err(e) => {
                    tracing::warn!(source = %reference, error = %e, "Skipping invalid document in multimodal pipeline");
                    skipped_attachments.push(SkippedAttachment {
                        url: reference.clone(),
                        kind: "document",
                        reason: e.to_string(),
                    });
                }
            }
        }

        if image_refs.is_empty() && doc_refs.is_empty() {
            normalized_messages.push(message.clone());
            continue;
        }

        if !normalized_image_refs.is_empty() {
            has_images = true;
        }
        if !normalized_doc_refs.is_empty() {
            has_documents = true;
        }

        let content = compose_multimodal_message_full(
            &text_after_docs,
            &image_refs,
            &normalized_image_refs,
            &doc_refs,
            &normalized_doc_refs,
            &skipped_attachments,
        );
        normalized_messages.push(ChatMessage {
            role: message.role.clone(),
            content,
        });
    }

    Ok(PreparedMessages {
        messages: normalized_messages,
        contains_images: has_images,
        contains_documents: has_documents,
    })
}

/// Strip image markers from older messages (oldest first) until total image
/// count is within `max_images`. Keeps the text content of each message.
fn trim_old_images(messages: &[ChatMessage], max_images: usize) -> Vec<ChatMessage> {
    // Find which messages (by index) contain images, oldest first.
    let image_positions: Vec<(usize, usize)> = messages
        .iter()
        .enumerate()
        .filter(|(_, m)| m.role == "user")
        .filter_map(|(i, m)| {
            let count = parse_image_markers(&m.content).1.len();
            if count > 0 { Some((i, count)) } else { None }
        })
        .collect();

    // Determine how many images to drop (from the oldest messages).
    let total: usize = image_positions.iter().map(|(_, c)| c).sum();
    let mut to_drop = total.saturating_sub(max_images);

    // Collect indices of messages whose images should be stripped.
    let mut strip_indices = std::collections::HashSet::new();
    for &(idx, count) in &image_positions {
        if to_drop == 0 {
            break;
        }
        strip_indices.insert(idx);
        to_drop = to_drop.saturating_sub(count);
    }

    messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            if strip_indices.contains(&i) {
                let (cleaned, _) = parse_image_markers(&m.content);
                let text = if cleaned.trim().is_empty() {
                    "[image removed from history]".to_string()
                } else {
                    cleaned
                };
                ChatMessage {
                    role: m.role.clone(),
                    content: text,
                }
            } else {
                m.clone()
            }
        })
        .collect()
}

fn compose_multimodal_message_full(
    text: &str,
    original_image_urls: &[String],
    image_uris: &[String],
    original_doc_urls: &[String],
    document_uris: &[String],
    skipped: &[SkippedAttachment],
) -> String {
    let mut content = String::new();
    let trimmed = text.trim();

    if !trimmed.is_empty() {
        content.push_str(trimmed);
        content.push_str("\n\n");
    }

    // Preserve original URLs as text so the model can reference them
    // (e.g., to forward a file URL to Slack or another skill).
    let mut url_refs: Vec<String> = Vec::new();
    for url in original_image_urls {
        if url.starts_with("http://") || url.starts_with("https://") {
            url_refs.push(format!("- Attached image: {url}"));
        }
    }
    for url in original_doc_urls {
        if url.starts_with("http://") || url.starts_with("https://") {
            url_refs.push(format!("- Attached document: {url}"));
        }
    }
    if !url_refs.is_empty() {
        content.push_str("Attachments:\n");
        content.push_str(&url_refs.join("\n"));
        content.push_str("\n\n");
    }

    // Report skipped attachments with details
    if !skipped.is_empty() {
        content.push_str("Skipped attachments (could not be processed):\n");
        for s in skipped {
            content.push_str(&format!("- {} {}: {}\n", s.kind, s.url, s.reason));
        }
        content.push('\n');
    }

    for (index, data_uri) in image_uris.iter().enumerate() {
        if index > 0 {
            content.push('\n');
        }
        content.push_str(IMAGE_MARKER_PREFIX);
        content.push_str(data_uri);
        content.push(']');
    }

    if !image_uris.is_empty() && !document_uris.is_empty() {
        content.push('\n');
    }

    const INLINE_TEXT_PREFIX: &str = "[INLINE_TEXT:";
    for (index, data_uri) in document_uris.iter().enumerate() {
        if index > 0 {
            content.push('\n');
        }
        if data_uri.starts_with(INLINE_TEXT_PREFIX) && data_uri.ends_with(']') {
            // Text-based document (CSV, plain text) — inline as text, not document block
            let text_content = &data_uri[INLINE_TEXT_PREFIX.len()..data_uri.len() - 1];
            content.push_str("File content:\n```\n");
            content.push_str(text_content);
            content.push_str("\n```");
        } else {
            content.push_str(DOCUMENT_MARKER_PREFIX);
            content.push_str(data_uri);
            content.push(']');
        }
    }

    content
}

async fn normalize_image_reference(
    source: &str,
    config: &MultimodalConfig,
    max_bytes: usize,
    remote_client: &Client,
) -> anyhow::Result<String> {
    if source.starts_with("data:") {
        return normalize_data_uri(source, max_bytes);
    }

    if source.starts_with("http://") || source.starts_with("https://") {
        if !config.allow_remote_fetch {
            return Err(MultimodalError::RemoteFetchDisabled {
                input: source.to_string(),
            }
            .into());
        }

        return normalize_remote_image(source, max_bytes, remote_client).await;
    }

    normalize_local_image(source, max_bytes).await
}

fn normalize_data_uri(source: &str, max_bytes: usize) -> anyhow::Result<String> {
    let Some(comma_idx) = source.find(',') else {
        return Err(MultimodalError::InvalidMarker {
            input: source.to_string(),
            reason: "expected data URI payload".to_string(),
        }
        .into());
    };

    let header = &source[..comma_idx];
    let payload = source[comma_idx + 1..].trim();

    if !header.contains(";base64") {
        return Err(MultimodalError::InvalidMarker {
            input: source.to_string(),
            reason: "only base64 data URIs are supported".to_string(),
        }
        .into());
    }

    let mime = header
        .trim_start_matches("data:")
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    validate_mime(source, &mime)?;

    let decoded = STANDARD
        .decode(payload)
        .map_err(|error| MultimodalError::InvalidMarker {
            input: source.to_string(),
            reason: format!("invalid base64 payload: {error}"),
        })?;

    validate_size(source, decoded.len(), max_bytes)?;

    Ok(format!("data:{mime};base64,{}", STANDARD.encode(decoded)))
}

async fn normalize_remote_image(
    source: &str,
    max_bytes: usize,
    remote_client: &Client,
) -> anyhow::Result<String> {
    let response = remote_client.get(source).send().await.map_err(|error| {
        MultimodalError::RemoteFetchFailed {
            input: source.to_string(),
            reason: error.to_string(),
        }
    })?;

    let status = response.status();
    if !status.is_success() {
        return Err(MultimodalError::RemoteFetchFailed {
            input: source.to_string(),
            reason: format!("HTTP {status}"),
        }
        .into());
    }

    if let Some(content_length) = response.content_length() {
        let content_length = usize::try_from(content_length).unwrap_or(usize::MAX);
        validate_size(source, content_length, max_bytes)?;
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);

    let bytes = response
        .bytes()
        .await
        .map_err(|error| MultimodalError::RemoteFetchFailed {
            input: source.to_string(),
            reason: error.to_string(),
        })?;

    validate_size(source, bytes.len(), max_bytes)?;

    let mime = detect_mime(None, bytes.as_ref(), content_type.as_deref()).ok_or_else(|| {
        MultimodalError::UnsupportedMime {
            input: source.to_string(),
            mime: "unknown".to_string(),
        }
    })?;

    validate_mime(source, &mime)?;

    Ok(format!("data:{mime};base64,{}", STANDARD.encode(bytes)))
}

async fn normalize_local_image(source: &str, max_bytes: usize) -> anyhow::Result<String> {
    let path = Path::new(source);
    if !path.exists() || !path.is_file() {
        return Err(MultimodalError::ImageSourceNotFound {
            input: source.to_string(),
        }
        .into());
    }

    let metadata =
        tokio::fs::metadata(path)
            .await
            .map_err(|error| MultimodalError::LocalReadFailed {
                input: source.to_string(),
                reason: error.to_string(),
            })?;

    validate_size(
        source,
        usize::try_from(metadata.len()).unwrap_or(usize::MAX),
        max_bytes,
    )?;

    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| MultimodalError::LocalReadFailed {
            input: source.to_string(),
            reason: error.to_string(),
        })?;

    validate_size(source, bytes.len(), max_bytes)?;

    let mime =
        detect_mime(Some(path), &bytes, None).ok_or_else(|| MultimodalError::UnsupportedMime {
            input: source.to_string(),
            mime: "unknown".to_string(),
        })?;

    validate_mime(source, &mime)?;

    Ok(format!("data:{mime};base64,{}", STANDARD.encode(bytes)))
}

async fn normalize_remote_document(
    source: &str,
    config: &MultimodalConfig,
    remote_client: &Client,
) -> anyhow::Result<String> {
    // Data URIs are passed through with validation only
    if source.starts_with("data:") {
        let Some(comma_idx) = source.find(',') else {
            return Err(MultimodalError::InvalidMarker {
                input: source.to_string(),
                reason: "expected data URI payload".to_string(),
            }
            .into());
        };
        let header = &source[..comma_idx];
        let mime = header
            .trim_start_matches("data:")
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        validate_document_mime(source, &mime)?;
        return Ok(source.to_string());
    }

    if source.starts_with("http://") || source.starts_with("https://") {
        if !config.allow_remote_fetch {
            return Err(MultimodalError::RemoteFetchDisabled {
                input: source.to_string(),
            }
            .into());
        }

        let response = remote_client.get(source).send().await.map_err(|error| {
            MultimodalError::RemoteFetchFailed {
                input: source.to_string(),
                reason: error.to_string(),
            }
        })?;

        let status = response.status();
        if !status.is_success() {
            return Err(MultimodalError::RemoteFetchFailed {
                input: source.to_string(),
                reason: format!("HTTP {status}"),
            }
            .into());
        }

        if let Some(content_length) = response.content_length() {
            let content_length = usize::try_from(content_length).unwrap_or(usize::MAX);
            validate_size(source, content_length, MAX_DOCUMENT_BYTES)?;
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);

        let bytes = response
            .bytes()
            .await
            .map_err(|error| MultimodalError::RemoteFetchFailed {
                input: source.to_string(),
                reason: error.to_string(),
            })?;

        validate_size(source, bytes.len(), MAX_DOCUMENT_BYTES)?;

        let mime = detect_document_mime(source, None, bytes.as_ref(), content_type.as_deref())?;
        return finalize_document(source, &mime, &bytes).await;
    }

    // Local file path
    let path = Path::new(source);
    if !path.exists() || !path.is_file() {
        return Err(MultimodalError::ImageSourceNotFound {
            input: source.to_string(),
        }
        .into());
    }

    let metadata =
        tokio::fs::metadata(path)
            .await
            .map_err(|error| MultimodalError::LocalReadFailed {
                input: source.to_string(),
                reason: error.to_string(),
            })?;

    validate_size(
        source,
        usize::try_from(metadata.len()).unwrap_or(usize::MAX),
        MAX_DOCUMENT_BYTES,
    )?;

    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| MultimodalError::LocalReadFailed {
            input: source.to_string(),
            reason: error.to_string(),
        })?;

    validate_size(source, bytes.len(), MAX_DOCUMENT_BYTES)?;

    let mime = detect_document_mime(source, Some(path), &bytes, None)?;
    finalize_document(source, &mime, &bytes).await
}

fn validate_document_mime(source: &str, mime: &str) -> anyhow::Result<()> {
    if ALLOWED_DOCUMENT_MIME_TYPES.contains(&mime) {
        return Ok(());
    }
    Err(MultimodalError::UnsupportedMime {
        input: source.to_string(),
        mime: mime.to_string(),
    }
    .into())
}

/// Detect the MIME type of a document, accepting PDF, text, CSV, and Office formats.
fn detect_document_mime(
    source: &str,
    path: Option<&Path>,
    bytes: &[u8],
    header_content_type: Option<&str>,
) -> anyhow::Result<String> {
    // Try extension first (most reliable for Office formats — they share ZIP magic bytes)
    if let Some(p) = path.or_else(|| {
        // Extract filename from URL path
        let url_path = source.split('?').next().unwrap_or(source);
        let name = url_path.rsplit('/').next().unwrap_or("");
        if name.contains('.') {
            Some(Path::new(name))
        } else {
            None
        }
    }) && let Some(ext) = p.extension().and_then(|e| e.to_str())
        && let Some(mime) = mime_from_extension(ext)
    {
        return Ok(mime.to_string());
    }

    // Fall back to Content-Type header
    if let Some(header_mime) = header_content_type.and_then(normalize_content_type) {
        return Ok(header_mime);
    }

    // Fall back to magic bytes
    if let Some(magic_mime) = mime_from_magic(bytes) {
        return Ok(magic_mime.to_string());
    }

    Err(MultimodalError::UnsupportedMime {
        input: source.to_string(),
        mime: "unknown".to_string(),
    }
    .into())
}

/// Finalize document bytes into a data URI. Converts Office formats (DOCX/XLSX/PPTX)
/// to PDF via office2pdf (on a blocking thread). CSV is treated as text/plain.
async fn finalize_document(source: &str, mime: &str, bytes: &[u8]) -> anyhow::Result<String> {
    // Direct pass-through for natively supported document types (PDF)
    if ALLOWED_DOCUMENT_MIME_TYPES.contains(&mime) {
        return Ok(format!("data:{mime};base64,{}", STANDARD.encode(bytes)));
    }

    // Text-based files (CSV, plain text) → inline as text, not document block.
    // Anthropic only accepts application/pdf for document blocks via base64.
    if TEXT_DOCUMENT_MIME_TYPES.contains(&mime) {
        let text = String::from_utf8_lossy(bytes);
        return Ok(format!("[INLINE_TEXT:{text}]"));
    }

    // Office formats → convert to PDF (when the feature is enabled) or reject
    // gracefully (when compiled without office-convert).
    #[cfg(feature = "office-convert")]
    {
        if let Some(&(_, format)) = CONVERTIBLE_OFFICE_MIME_TYPES
            .iter()
            .find(|&&(m, _)| m == mime)
        {
            let bytes_owned = bytes.to_vec();
            let source_owned = source.to_string();
            let result = tokio::task::spawn_blocking(move || {
                office2pdf::convert_bytes(
                    &bytes_owned,
                    format,
                    &office2pdf::config::ConvertOptions::default(),
                )
                .map_err(|e| MultimodalError::RemoteFetchFailed {
                    input: source_owned,
                    reason: format!("office2pdf conversion failed: {e}"),
                })
            })
            .await
            .map_err(|e| anyhow::anyhow!("spawn_blocking join error: {e}"))??;

            return Ok(format!(
                "data:application/pdf;base64,{}",
                STANDARD.encode(&result.pdf)
            ));
        }
    }

    #[cfg(not(feature = "office-convert"))]
    {
        if CONVERTIBLE_OFFICE_MIME_TYPES.contains(&mime) {
            return Err(MultimodalError::UnsupportedMime {
                input: source.to_string(),
                mime: format!("{mime} (office-convert feature not enabled)"),
            }
            .into());
        }
    }

    Err(MultimodalError::UnsupportedMime {
        input: source.to_string(),
        mime: mime.to_string(),
    }
    .into())
}

fn validate_size(source: &str, size_bytes: usize, max_bytes: usize) -> anyhow::Result<()> {
    if size_bytes > max_bytes {
        return Err(MultimodalError::ImageTooLarge {
            input: source.to_string(),
            size_bytes,
            max_bytes,
        }
        .into());
    }

    Ok(())
}

fn validate_mime(source: &str, mime: &str) -> anyhow::Result<()> {
    if ALLOWED_IMAGE_MIME_TYPES.contains(&mime) {
        return Ok(());
    }

    let mime_msg = if mime == "image/heic" || mime == "image/heif" {
        format!(
            "{mime} (HEIC/HEIF not supported by AI provider — ask user to resend as JPEG or PNG)"
        )
    } else {
        mime.to_string()
    };

    Err(MultimodalError::UnsupportedMime {
        input: source.to_string(),
        mime: mime_msg,
    }
    .into())
}

fn detect_mime(
    path: Option<&Path>,
    bytes: &[u8],
    header_content_type: Option<&str>,
) -> Option<String> {
    if let Some(header_mime) = header_content_type.and_then(normalize_content_type) {
        return Some(header_mime);
    }

    if let Some(path) = path
        && let Some(ext) = path.extension().and_then(|value| value.to_str())
        && let Some(mime) = mime_from_extension(ext)
    {
        return Some(mime.to_string());
    }

    mime_from_magic(bytes).map(ToString::to_string)
}

fn normalize_content_type(content_type: &str) -> Option<String> {
    let mime = content_type.split(';').next()?.trim().to_ascii_lowercase();
    if mime.is_empty() { None } else { Some(mime) }
}

fn mime_from_extension(ext: &str) -> Option<&'static str> {
    match ext.to_ascii_lowercase().as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "webp" => Some("image/webp"),
        "gif" => Some("image/gif"),
        "bmp" => Some("image/bmp"),
        "heic" | "heif" => Some("image/heic"),
        "pdf" => Some("application/pdf"),
        "txt" => Some("text/plain"),
        "csv" => Some("text/csv"),
        "docx" => Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        "xlsx" => Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        "pptx" => Some("application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        _ => None,
    }
}

fn mime_from_magic(bytes: &[u8]) -> Option<&'static str> {
    if bytes.len() >= 8 && bytes.starts_with(&[0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n']) {
        return Some("image/png");
    }

    if bytes.len() >= 3 && bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        return Some("image/jpeg");
    }

    if bytes.len() >= 6 && (bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a")) {
        return Some("image/gif");
    }

    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return Some("image/webp");
    }

    if bytes.len() >= 2 && bytes.starts_with(b"BM") {
        return Some("image/bmp");
    }

    // HEIC/HEIF: ISO BMFF container with ftyp box, brand starts at offset 8
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        let brand = &bytes[8..12];
        if brand == b"heic" || brand == b"heix" || brand == b"mif1" || brand == b"hevc" {
            return Some("image/heic");
        }
    }

    if bytes.len() >= 4 && bytes.starts_with(&[0x25, 0x50, 0x44, 0x46]) {
        return Some("application/pdf");
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_image_markers_extracts_multiple_markers() {
        let input = "Check this [IMAGE:/tmp/a.png] and this [IMAGE:https://example.com/b.jpg]";
        let (cleaned, refs) = parse_image_markers(input);

        assert_eq!(cleaned, "Check this  and this");
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0], "/tmp/a.png");
        assert_eq!(refs[1], "https://example.com/b.jpg");
    }

    #[test]
    fn parse_image_markers_keeps_invalid_empty_marker() {
        let input = "hello [IMAGE:] world";
        let (cleaned, refs) = parse_image_markers(input);

        assert_eq!(cleaned, "hello [IMAGE:] world");
        assert!(refs.is_empty());
    }

    #[tokio::test]
    async fn prepare_messages_normalizes_local_image_to_data_uri() {
        let temp = tempfile::tempdir().unwrap();
        let image_path = temp.path().join("sample.png");

        // Minimal PNG signature bytes are enough for MIME detection.
        std::fs::write(
            &image_path,
            [0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'],
        )
        .unwrap();

        let messages = vec![ChatMessage::user(format!(
            "Please inspect this screenshot [IMAGE:{}]",
            image_path.display()
        ))];

        let prepared = prepare_messages_for_provider(&messages, &MultimodalConfig::default())
            .await
            .unwrap();

        assert!(prepared.contains_images);
        assert_eq!(prepared.messages.len(), 1);

        let (cleaned, refs) = parse_image_markers(&prepared.messages[0].content);
        assert_eq!(cleaned, "Please inspect this screenshot");
        assert_eq!(refs.len(), 1);
        assert!(refs[0].starts_with("data:image/png;base64,"));
    }

    #[tokio::test]
    async fn prepare_messages_trims_excess_images_from_older_messages() {
        // 3 messages, each with 1 image — max is 2.
        // The oldest message's image should be stripped.
        let messages = vec![
            ChatMessage::user("[IMAGE:/tmp/old.png]\nOld caption".to_string()),
            ChatMessage::user("[IMAGE:/tmp/mid.png]\nMid caption".to_string()),
            ChatMessage::user("[IMAGE:/tmp/new.png]\nNew caption".to_string()),
        ];

        // Should not error — instead trims oldest.
        // (Will error on normalize_image_reference for the surviving images
        //  since /tmp/mid.png and /tmp/new.png don't exist, but the trimming
        //  itself should succeed.)
        let trimmed = trim_old_images(&messages, 2);
        assert_eq!(trimmed.len(), 3);

        // Oldest message should have image stripped
        let (_, refs0) = parse_image_markers(&trimmed[0].content);
        assert!(refs0.is_empty(), "oldest image should be stripped");
        assert!(trimmed[0].content.contains("Old caption"));

        // Newer messages keep their images
        let (_, refs1) = parse_image_markers(&trimmed[1].content);
        assert_eq!(refs1.len(), 1);
        let (_, refs2) = parse_image_markers(&trimmed[2].content);
        assert_eq!(refs2.len(), 1);
    }

    #[test]
    fn trim_old_images_replaces_image_only_message() {
        // A message with only an image and no text should get a placeholder.
        let messages = vec![
            ChatMessage::user("[IMAGE:/tmp/old.png]".to_string()),
            ChatMessage::user("[IMAGE:/tmp/new.png]\nKeep this".to_string()),
        ];

        let trimmed = trim_old_images(&messages, 1);
        assert_eq!(trimmed[0].content, "[image removed from history]");
        assert!(trimmed[1].content.contains("[IMAGE:/tmp/new.png]"));
    }

    #[test]
    fn trim_old_images_multi_image_message_stripped_as_unit() {
        // A single message has 3 images. We need to drop 2 to reach max=1.
        // But trimming works at message granularity — the entire message gets
        // stripped (all 3 images removed), which over-trims to 0. The newest
        // message (text-only) is untouched.
        let messages = vec![
            ChatMessage::user(
                "[IMAGE:/tmp/a.png]\n[IMAGE:/tmp/b.png]\n[IMAGE:/tmp/c.png]\nThree pics"
                    .to_string(),
            ),
            ChatMessage::user("Just text, no images".to_string()),
        ];

        let trimmed = trim_old_images(&messages, 1);
        assert_eq!(trimmed.len(), 2);
        // All images in the first message are gone, but text remains
        let (_, refs0) = parse_image_markers(&trimmed[0].content);
        assert!(refs0.is_empty());
        assert!(trimmed[0].content.contains("Three pics"));
        // Second message unchanged
        assert_eq!(trimmed[1].content, "Just text, no images");
    }

    #[test]
    fn trim_old_images_skips_assistant_messages() {
        // Assistant messages with image markers should not be counted or stripped.
        let messages = vec![
            ChatMessage {
                role: "assistant".to_string(),
                content: "[IMAGE:/tmp/assistant.png]\nAssistant generated".to_string(),
            },
            ChatMessage::user("[IMAGE:/tmp/user1.png]\nFirst".to_string()),
            ChatMessage::user("[IMAGE:/tmp/user2.png]\nSecond".to_string()),
        ];

        let trimmed = trim_old_images(&messages, 1);
        // Assistant message untouched (not counted toward limit)
        assert!(trimmed[0].content.contains("[IMAGE:/tmp/assistant.png]"));
        // Oldest user image stripped
        let (_, refs1) = parse_image_markers(&trimmed[1].content);
        assert!(refs1.is_empty());
        assert!(trimmed[1].content.contains("First"));
        // Newest user image kept
        let (_, refs2) = parse_image_markers(&trimmed[2].content);
        assert_eq!(refs2.len(), 1);
    }

    #[test]
    fn trim_old_images_no_trimming_when_under_limit() {
        let messages = vec![
            ChatMessage::user("[IMAGE:/tmp/a.png]\nCaption A".to_string()),
            ChatMessage::user("[IMAGE:/tmp/b.png]\nCaption B".to_string()),
        ];

        let trimmed = trim_old_images(&messages, 5);
        // Nothing should change — both images are under the limit
        assert_eq!(trimmed[0].content, messages[0].content);
        assert_eq!(trimmed[1].content, messages[1].content);
    }

    #[test]
    fn trim_old_images_no_trimming_when_exactly_at_limit() {
        let messages = vec![
            ChatMessage::user("[IMAGE:/tmp/a.png]\nA".to_string()),
            ChatMessage::user("[IMAGE:/tmp/b.png]\nB".to_string()),
        ];

        let trimmed = trim_old_images(&messages, 2);
        assert_eq!(trimmed[0].content, messages[0].content);
        assert_eq!(trimmed[1].content, messages[1].content);
    }

    #[test]
    fn trim_old_images_empty_messages() {
        let trimmed = trim_old_images(&[], 4);
        assert!(trimmed.is_empty());
    }

    #[test]
    fn trim_old_images_interleaved_roles() {
        // Realistic conversation: user sends image, assistant replies, user sends
        // another image, etc. Only user messages should be candidates for trimming.
        let messages = vec![
            ChatMessage::user("[IMAGE:/tmp/1.png]\nLook at this".to_string()),
            ChatMessage {
                role: "assistant".to_string(),
                content: "I see a photo.".to_string(),
            },
            ChatMessage::user("[IMAGE:/tmp/2.png]\nWhat about this?".to_string()),
            ChatMessage {
                role: "assistant".to_string(),
                content: "That's a chart.".to_string(),
            },
            ChatMessage::user("[IMAGE:/tmp/3.png]\nAnd this one".to_string()),
        ];

        let trimmed = trim_old_images(&messages, 2);
        assert_eq!(trimmed.len(), 5);
        // Oldest user image stripped
        let (_, refs0) = parse_image_markers(&trimmed[0].content);
        assert!(refs0.is_empty());
        assert!(trimmed[0].content.contains("Look at this"));
        // Assistant messages untouched
        assert_eq!(trimmed[1].content, "I see a photo.");
        assert_eq!(trimmed[3].content, "That's a chart.");
        // Two newest user images kept
        let (_, refs2) = parse_image_markers(&trimmed[2].content);
        assert_eq!(refs2.len(), 1);
        let (_, refs4) = parse_image_markers(&trimmed[4].content);
        assert_eq!(refs4.len(), 1);
    }

    #[test]
    fn trim_old_images_strips_multiple_oldest_messages() {
        // 5 user images, max 1 — should strip the first 4 messages' images.
        let messages: Vec<ChatMessage> = (1..=5)
            .map(|i| ChatMessage::user(format!("[IMAGE:/tmp/{i}.png]\nCaption {i}")))
            .collect();

        let trimmed = trim_old_images(&messages, 1);
        assert_eq!(trimmed.len(), 5);
        for (i, msg) in trimmed.iter().enumerate().take(4) {
            let (_, refs) = parse_image_markers(&msg.content);
            assert!(refs.is_empty(), "message {i} should have images stripped");
            assert!(msg.content.contains(&format!("Caption {}", i + 1)));
        }
        // Only the last message keeps its image
        let (_, refs_last) = parse_image_markers(&trimmed[4].content);
        assert_eq!(refs_last.len(), 1);
    }

    #[tokio::test]
    async fn prepare_messages_trims_then_normalizes_surviving_images() {
        // End-to-end: 3 images, max 2. After trimming the oldest, the two
        // surviving images should be normalized (base64-encoded) successfully.
        let temp = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        for name in ["old.png", "mid.png", "new.png"] {
            let p = temp.path().join(name);
            // Minimal valid PNG (1x1 white pixel)
            let png_data = [
                0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // PNG signature
                0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52, // IHDR chunk
                0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00, 0x00, 0x90,
                0x77, 0x53, 0xDE, // 1x1 RGB
                0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, // IDAT chunk
                0x08, 0xD7, 0x63, 0xF8, 0xCF, 0xC0, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0xE2, 0x21,
                0xBC, 0x33, // IDAT data + CRC
                0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, // IEND chunk
                0xAE, 0x42, 0x60, 0x82,
            ];
            std::fs::write(&p, png_data).unwrap();
            paths.push(p);
        }

        let messages = vec![
            ChatMessage::user(format!("[IMAGE:{}]\nOld", paths[0].display())),
            ChatMessage::user(format!("[IMAGE:{}]\nMid", paths[1].display())),
            ChatMessage::user(format!("[IMAGE:{}]\nNew", paths[2].display())),
        ];

        let config = MultimodalConfig {
            max_images: 2,
            max_image_size_mb: 5,
            allow_remote_fetch: false,
            ..Default::default()
        };

        let result = prepare_messages_for_provider(&messages, &config)
            .await
            .expect("should succeed after trimming");

        assert!(result.contains_images);
        assert_eq!(result.messages.len(), 3);
        // First message should have image stripped, text preserved
        assert!(!result.messages[0].content.contains("data:image"));
        assert!(result.messages[0].content.contains("Old"));
        // Second and third should have base64-encoded images
        assert!(result.messages[1].content.contains("data:image"));
        assert!(result.messages[2].content.contains("data:image"));
    }

    #[tokio::test]
    async fn prepare_messages_skips_remote_url_when_disabled() {
        let messages = vec![ChatMessage::user(
            "Look [IMAGE:https://example.com/img.png]".to_string(),
        )];

        let result = prepare_messages_for_provider(&messages, &MultimodalConfig::default())
            .await
            .expect("should succeed — remote image skipped gracefully");

        // The image was skipped, so the message should contain the skip placeholder
        assert!(
            result.messages[0]
                .content
                .contains("Skipped attachments (could not be processed)")
        );
    }

    #[tokio::test]
    async fn prepare_messages_skips_oversized_image_gracefully() {
        let temp = tempfile::tempdir().unwrap();
        let image_path = temp.path().join("big.png");

        // Write a file larger than 1 MB with PNG magic bytes so MIME detection works
        let mut bytes = vec![0u8; 1024 * 1024 + 1];
        bytes[..8].copy_from_slice(&[0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n']);
        std::fs::write(&image_path, bytes).unwrap();

        let messages = vec![ChatMessage::user(format!(
            "Check this [IMAGE:{}]",
            image_path.display()
        ))];
        let config = MultimodalConfig {
            max_images: 4,
            max_image_size_mb: 1,
            allow_remote_fetch: false,
            ..Default::default()
        };

        let result = prepare_messages_for_provider(&messages, &config)
            .await
            .expect("should succeed — oversized image skipped gracefully");

        assert!(
            result.messages[0]
                .content
                .contains("Skipped attachments (could not be processed)")
        );
        assert!(result.messages[0].content.contains("Check this"));
    }

    #[test]
    fn extract_ollama_image_payload_supports_data_uris() {
        let payload = extract_ollama_image_payload("data:image/png;base64,abcd==")
            .expect("payload should be extracted");
        assert_eq!(payload, "abcd==");
    }

    /// Stripping `[IMAGE:]` markers from history messages leaves only the text
    /// portion, which is the behaviour needed for non-vision providers (#3674).
    #[test]
    fn parse_image_markers_strips_markers_leaving_caption() {
        let input = "[IMAGE:/tmp/photo.jpg]\n\nDescribe this screenshot";
        let (cleaned, refs) = parse_image_markers(input);
        assert_eq!(cleaned, "Describe this screenshot");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], "/tmp/photo.jpg");
    }

    /// An image-only message (no caption) should produce an empty string after
    /// marker stripping, so callers can drop it from history.
    #[test]
    fn parse_image_markers_image_only_message_becomes_empty() {
        let input = "[IMAGE:/tmp/photo.jpg]";
        let (cleaned, refs) = parse_image_markers(input);
        assert!(
            cleaned.is_empty(),
            "expected empty string, got: {cleaned:?}"
        );
        assert_eq!(refs.len(), 1);
    }

    #[test]
    fn parse_document_markers_extracts_markers() {
        let input = "Review [DOCUMENT:https://example.com/contract.pdf] please";
        let (cleaned, refs) = parse_document_markers(input);
        assert_eq!(cleaned, "Review  please");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], "https://example.com/contract.pdf");
    }

    #[test]
    fn parse_document_markers_empty_marker_kept() {
        let input = "hello [DOCUMENT:] world";
        let (cleaned, refs) = parse_document_markers(input);
        assert_eq!(cleaned, "hello [DOCUMENT:] world");
        assert!(refs.is_empty());
    }

    #[test]
    fn mime_from_extension_detects_pdf() {
        assert_eq!(mime_from_extension("pdf"), Some("application/pdf"));
    }

    #[test]
    fn mime_from_magic_detects_pdf() {
        let pdf_bytes = b"%PDF-1.4 some content";
        assert_eq!(mime_from_magic(pdf_bytes), Some("application/pdf"));
    }

    #[tokio::test]
    async fn prepare_messages_normalizes_local_pdf_document() {
        let temp = tempfile::tempdir().unwrap();
        let pdf_path = temp.path().join("test.pdf");
        // Minimal PDF magic bytes
        std::fs::write(&pdf_path, b"%PDF-1.4 minimal").unwrap();

        let messages = vec![ChatMessage::user(format!(
            "Review this doc [DOCUMENT:{}]",
            pdf_path.display()
        ))];

        let prepared = prepare_messages_for_provider(&messages, &MultimodalConfig::default())
            .await
            .unwrap();

        assert!(prepared.contains_documents);
        let (_, doc_refs) = parse_document_markers(&prepared.messages[0].content);
        assert_eq!(doc_refs.len(), 1);
        assert!(doc_refs[0].starts_with("data:application/pdf;base64,"));
    }

    #[tokio::test]
    async fn finalize_document_converts_csv_to_inline_text() {
        let csv = b"name,email\nJohn,john@test.com\n";
        let result = finalize_document("test.csv", "text/csv", csv)
            .await
            .unwrap();
        assert!(result.starts_with("[INLINE_TEXT:"));
        assert!(result.contains("name,email"));
    }

    #[tokio::test]
    async fn finalize_document_passes_through_pdf() {
        let pdf = b"%PDF-1.4 minimal";
        let result = finalize_document("test.pdf", "application/pdf", pdf)
            .await
            .unwrap();
        assert!(result.starts_with("data:application/pdf;base64,"));
    }

    #[tokio::test]
    async fn finalize_document_rejects_unknown_mime() {
        let result = finalize_document("test.xyz", "application/x-unknown", b"data").await;
        assert!(result.is_err());
    }

    #[test]
    fn detect_document_mime_from_url_extension() {
        let mime = detect_document_mime(
            "https://cdn.example.com/report.xlsx?token=abc",
            None,
            &[0x50, 0x4B, 0x03, 0x04], // ZIP magic
            None,
        )
        .unwrap();
        assert_eq!(
            mime,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
    }

    #[test]
    fn detect_document_mime_csv_from_extension() {
        let mime = detect_document_mime(
            "https://cdn.example.com/data.csv",
            None,
            b"col1,col2\n",
            None,
        )
        .unwrap();
        assert_eq!(mime, "text/csv");
    }

    // ── URL extraction tests ────────────────────────────────────

    #[test]
    fn inject_url_markers_detects_image_urls() {
        let input = "Check this https://cdn.example.com/photo.png please";
        let result = inject_url_markers(input);
        assert!(result.contains("[IMAGE:https://cdn.example.com/photo.png]"));
    }

    #[test]
    fn inject_url_markers_detects_document_urls() {
        let input = "Here is the report https://files.example.com/report.pdf";
        let result = inject_url_markers(input);
        assert!(result.contains("[DOCUMENT:https://files.example.com/report.pdf]"));
    }

    #[test]
    fn inject_url_markers_detects_csv() {
        let input = "Download https://storage.example.com/data.csv for analysis";
        let result = inject_url_markers(input);
        assert!(result.contains("[DOCUMENT:https://storage.example.com/data.csv]"));
    }

    #[test]
    fn inject_url_markers_detects_office_formats() {
        let input = "See https://a.com/file.docx and https://b.com/file.xlsx";
        let result = inject_url_markers(input);
        assert!(result.contains("[DOCUMENT:https://a.com/file.docx]"));
        assert!(result.contains("[DOCUMENT:https://b.com/file.xlsx]"));
    }

    #[test]
    fn inject_url_markers_skips_urls_already_in_markers() {
        let input = "Look [IMAGE:https://cdn.example.com/photo.png] done";
        let result = inject_url_markers(input);
        // Should NOT double-wrap
        assert!(!result.contains("[IMAGE:[IMAGE:"));
        assert_eq!(
            result.matches("[IMAGE:").count(),
            1,
            "should not duplicate marker"
        );
    }

    #[test]
    fn inject_url_markers_handles_heic() {
        let input = "Photo https://cdn.example.com/IMG_1234.heic from iPhone";
        let result = inject_url_markers(input);
        assert!(result.contains("HEIC/HEIF format is not supported"));
        assert!(!result.contains("[IMAGE:"));
    }

    #[test]
    fn inject_url_markers_ignores_urls_without_extension() {
        let input = "Visit https://example.com/api/data for info";
        let result = inject_url_markers(input);
        assert_eq!(result, input, "no markers should be injected");
    }

    #[test]
    fn inject_url_markers_handles_query_strings() {
        let input = "See https://cdn.example.com/photo.jpg?token=abc123&size=large please";
        let result = inject_url_markers(input);
        assert!(
            result.contains("[IMAGE:https://cdn.example.com/photo.jpg?token=abc123&size=large]")
        );
    }

    #[test]
    fn inject_url_markers_case_insensitive() {
        let input = "File at https://cdn.example.com/FILE.PNG here";
        let result = inject_url_markers(input);
        assert!(result.contains("[IMAGE:https://cdn.example.com/FILE.PNG]"));
    }

    #[test]
    fn inject_url_markers_same_url_inside_and_outside_marker() {
        // Same URL appears twice: once inside a marker (skip), once outside (inject)
        let input =
            "[IMAGE:https://cdn.example.com/photo.png] and also https://cdn.example.com/photo.png";
        let result = inject_url_markers(input);
        // The one inside the marker should NOT get double-wrapped
        assert!(result.starts_with("[IMAGE:https://cdn.example.com/photo.png]"));
        // The one outside should get a marker
        assert!(result.ends_with(
            "https://cdn.example.com/photo.png [IMAGE:https://cdn.example.com/photo.png]"
        ));
    }

    #[test]
    fn inject_url_markers_multiple_urls() {
        let input = "https://a.com/img.jpg https://b.com/doc.pdf https://c.com/page.html";
        let result = inject_url_markers(input);
        assert!(result.contains("[IMAGE:https://a.com/img.jpg]"));
        assert!(result.contains("[DOCUMENT:https://b.com/doc.pdf]"));
        // .html is not supported, should not be tagged
        assert!(!result.contains("[IMAGE:https://c.com/page.html]"));
        assert!(!result.contains("[DOCUMENT:https://c.com/page.html]"));
    }

    // ── HEIC detection tests ────────────────────────────────────

    #[test]
    fn heic_extension_detected() {
        assert_eq!(mime_from_extension("heic"), Some("image/heic"));
        assert_eq!(mime_from_extension("heif"), Some("image/heic"));
        assert_eq!(mime_from_extension("HEIC"), Some("image/heic"));
    }

    #[test]
    fn heic_magic_bytes_detected() {
        // Minimal HEIC ftyp box: 4 bytes size + "ftyp" + "heic" brand
        let heic_bytes: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x18, // size = 24
            b'f', b't', b'y', b'p', // type = ftyp
            b'h', b'e', b'i', b'c', // brand = heic
        ];
        assert_eq!(mime_from_magic(&heic_bytes), Some("image/heic"));
    }

    #[test]
    fn heic_validate_mime_fails_with_helpful_message() {
        let err = validate_mime("test.heic", "image/heic").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("HEIC/HEIF not supported"), "got: {msg}");
        assert!(msg.contains("JPEG or PNG"), "got: {msg}");
    }
}
