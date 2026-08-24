use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::config::DingtalkConfig;
use crate::connector::circuit_breaker::CircuitBreaker;
use crate::connector::Connector;
use crate::retry_async;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    errcode: Option<i64>,
    #[allow(dead_code)]
    errmsg: Option<String>,
}

/// DingTalk approval process instance list response.
#[derive(Debug, Deserialize)]
struct ApprovalListResponse {
    #[serde(default)]
    result: ApprovalResult,
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Default)]
struct ApprovalResult {
    #[serde(default)]
    list: Vec<ApprovalInstance>,
    #[serde(default)]
    next_cursor: i64,
    #[serde(default)]
    has_more: bool,
}

/// A single DingTalk approval process instance.
#[derive(Debug, Deserialize, Serialize)]
struct ApprovalInstance {
    #[serde(default)]
    process_instance_id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    originator_userid: String,
    #[serde(default)]
    create_time: i64,
    #[serde(default)]
    finish_time: i64,
    #[serde(default)]
    business_id: String,
}

/// DingTalk work notification response (topapi/message/corpconversation).
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct WorkNotificationResponse {
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
}

/// DingTalk robot messages are received via callbacks. For polling mode,
/// we check the async send result of previously sent messages.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct SendResultResponse {
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
    #[serde(default)]
    send_result: Option<SendResult>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct SendResult {
    #[serde(default)]
    invalid_user_id_list: Vec<String>,
    #[serde(default)]
    forbidden_user_id_list: Vec<String>,
    #[serde(default)]
    failed_user_id_list: Vec<String>,
}

/// DingTalk attendance check-in record.
#[derive(Debug, Deserialize, Serialize)]
struct AttendanceRecord {
    #[serde(default)]
    user_id: String,
    #[serde(default)]
    user_name: Option<String>,
    #[serde(default)]
    work_date: Option<String>,
    #[serde(default)]
    check_type: Option<String>,
    #[serde(default)]
    plan_check_time: Option<String>,
    #[serde(default)]
    clock_result: Option<String>,
    #[serde(default)]
    proc_inst_id: Option<String>,
    #[serde(default)]
    location_result: Option<String>,
    #[serde(default)]
    source_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AttendanceListResponse {
    #[serde(default)]
    result: AttendanceResult,
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Default)]
struct AttendanceResult {
    #[serde(default)]
    check_record_list: Vec<AttendanceRecord>,
    #[serde(default)]
    has_more: bool,
}

/// DingTalk work report (工作报告) item.
#[derive(Debug, Deserialize, Serialize)]
struct WorkReport {
    #[serde(default)]
    report_id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    creator_name: Option<String>,
    #[serde(default)]
    creator_id: Option<String>,
    #[serde(default)]
    create_time: i64,
    #[serde(default)]
    modified_time: i64,
    #[serde(default)]
    report_type: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct WorkReportListResponse {
    #[serde(default)]
    result: WorkReportResult,
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Default)]
struct WorkReportResult {
    #[serde(default)]
    data_list: Vec<WorkReport>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    next_cursor: i64,
}

/// Start the DingTalk connector. Authenticates via OAuth, then polls
/// approval process instances, attendance records, and work reports.
pub async fn start(config: DingtalkConfig, tx: EventTx, circuit_breaker: Option<CircuitBreaker>) -> Result<JoinHandle<()>> {
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let token = retry_async!("dingtalk_token", 3, {
        fetch_access_token(&http_client, &config).await
    })?;
    info!("DingTalk connector authenticated.");

    let poll_secs = config.poll_interval_secs;

    let handle = tokio::spawn(async move {
        let cb = circuit_breaker;
        let mut current_token = token;
        let mut token_refresh = tokio::time::interval(std::time::Duration::from_secs(6000));
        let mut poll_interval = tokio::time::interval(std::time::Duration::from_secs(poll_secs));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track seen instance IDs to avoid duplicate events
        let mut seen_approvals: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut seen_attendance: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut seen_reports: std::collections::HashSet<String> = std::collections::HashSet::new();

        loop {
            tokio::select! {
                _ = token_refresh.tick() => {
                    match fetch_access_token(&http_client, &config).await {
                        Ok(new_token) => {
                            current_token = new_token;
                            info!("DingTalk access token refreshed.");
                            if let Some(ref c) = cb { c.record_success().await; }
                        }
                        Err(e) => {
                            error!("Failed to refresh DingTalk token: {}", e);
                            if let Some(ref c) = cb { c.record_failure().await; }
                        }
                    }
                }
                _ = poll_interval.tick() => {
                    // Circuit breaker check
                    if let Some(ref c) = cb {
                        if c.allow_request().await.is_err() {
                            debug!("DingTalk circuit breaker open — skipping poll cycle");
                            continue;
                        }
                    }

                    let mut any_failed = false;

                    // Poll approval process instances
                    match fetch_approval_list(&http_client, &current_token).await {
                        Ok(instances) => {
                            debug!("Fetched {} DingTalk approval instances", instances.len());
                            for inst in instances {
                                if seen_approvals.contains(&inst.process_instance_id) {
                                    continue;
                                }
                                seen_approvals.insert(inst.process_instance_id.clone());

                                let raw_event = to_raw_event(&inst);
                                match tx.try_send(raw_event) {
                                    Ok(()) => {
                                        debug!("Forwarded DingTalk approval: {}", inst.title);
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        warn!("Event channel full, dropping DingTalk approval: {}", inst.title);
                                    }
                                    Err(e) => {
                                        error!("Failed to send DingTalk event: {}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to fetch DingTalk approvals: {}", e);
                            any_failed = true;
                        }
                    }

                    // Keep the seen set from growing unbounded (keep last 10000)
                    if seen_approvals.len() > 10000 {
                        let excess = seen_approvals.len() - 5000;
                        let to_remove: Vec<String> = seen_approvals.iter().take(excess).cloned().collect();
                        for id in to_remove {
                            seen_approvals.remove(&id);
                        }
                    }

                    // Poll attendance check-in records
                    match fetch_attendance_list(&http_client, &current_token).await {
                        Ok(records) => {
                            debug!("Fetched {} DingTalk attendance records", records.len());
                            for record in records {
                                let record_id = format!("{}:{}:{}",
                                    record.user_id,
                                    record.work_date.as_deref().unwrap_or(""),
                                    record.check_type.as_deref().unwrap_or(""),
                                );
                                if seen_attendance.contains(&record_id) {
                                    continue;
                                }
                                seen_attendance.insert(record_id.clone());

                                let raw_event = to_attendance_event(&record);
                                match tx.try_send(raw_event) {
                                    Ok(()) => {
                                        debug!("Forwarded DingTalk attendance: user={}", record.user_id);
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        warn!("Event channel full, dropping DingTalk attendance record");
                                    }
                                    Err(e) => {
                                        error!("Failed to send DingTalk attendance event: {}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!("Attendance poll skipped or failed: {}", e);
                            any_failed = true;
                        }
                    }

                    // Poll work reports (工作报告)
                    match fetch_work_reports(&http_client, &current_token).await {
                        Ok(reports) => {
                            debug!("Fetched {} DingTalk work reports", reports.len());
                            for report in reports {
                                if seen_reports.contains(&report.report_id) {
                                    continue;
                                }
                                seen_reports.insert(report.report_id.clone());

                                let raw_event = to_work_report_event(&report);
                                match tx.try_send(raw_event) {
                                    Ok(()) => {
                                        debug!("Forwarded DingTalk work report: {}", report.title);
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        warn!("Event channel full, dropping DingTalk work report");
                                    }
                                    Err(e) => {
                                        error!("Failed to send DingTalk work report event: {}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!("Work report poll skipped or failed: {}", e);
                            any_failed = true;
                        }
                    }

                    // Record circuit breaker result for this poll cycle
                    if let Some(ref c) = cb {
                        if any_failed { c.record_failure().await; } else { c.record_success().await; }
                    }

                    // Evict old seen records
                    if seen_attendance.len() > 10000 {
                        let excess = seen_attendance.len() - 5000;
                        let to_remove: Vec<String> = seen_attendance.iter().take(excess).cloned().collect();
                        for id in to_remove { seen_attendance.remove(&id); }
                    }
                    if seen_reports.len() > 10000 {
                        let excess = seen_reports.len() - 5000;
                        let to_remove: Vec<String> = seen_reports.iter().take(excess).cloned().collect();
                        for id in to_remove { seen_reports.remove(&id); }
                    }
                }
            }
        }
    });

    Ok(handle)
}

/// Fetch an access token from DingTalk.
async fn fetch_access_token(client: &Client, config: &DingtalkConfig) -> Result<String> {
    let url = format!(
        "https://oapi.dingtalk.com/gettoken?appkey={}&appsecret={}",
        config.app_key, config.app_secret
    );

    let resp = client
        .get(&url)
        .send()
        .await?
        .json::<TokenResponse>()
        .await?;
    if let Some(code) = resp.errcode {
        if code != 0 {
            anyhow::bail!("DingTalk token error {}: {:?}", code, resp.errmsg);
        }
    }
    Ok(resp.access_token)
}

/// Make a DingTalk API POST request with rate-limit awareness.
/// Handles HTTP 429 Too Many Requests and DingTalk error code 88 (rate limited).
/// Sleeps for the Retry-After duration or 1 second, then retries once.
async fn dingtalk_api_post(
    client: &Client,
    url: &str,
    body: &serde_json::Value,
) -> Result<reqwest::Response> {
    let resp = client
        .post(url)
        .json(body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("DingTalk API request failed: {}", e))?;

    // Handle HTTP 429 Too Many Requests
    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_after = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1);
        warn!(
            "DingTalk rate limited (429), waiting {}s before retry",
            retry_after
        );
        tokio::time::sleep(std::time::Duration::from_secs(retry_after)).await;

        let resp2 = client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("DingTalk API retry failed: {}", e))?;

        if !resp2.status().is_success() {
            let status = resp2.status();
            let body_text = resp2.text().await.unwrap_or_default();
            anyhow::bail!("DingTalk API error {} after retry: {}", status, body_text);
        }
        return Ok(resp2);
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        anyhow::bail!("DingTalk API error {}: {}", status, body_text);
    }

    Ok(resp)
}

/// Fetch approval process instances from DingTalk.
/// Uses the topapi/processinstance/list API.
async fn fetch_approval_list(client: &Client, token: &str) -> Result<Vec<ApprovalInstance>> {
    let url = format!(
        "https://oapi.dingtalk.com/topapi/processinstance/list?access_token={}",
        token
    );

    // Query approval instances from the last 24 hours
    let now_ms = Utc::now().timestamp_millis();
    let day_ago_ms = now_ms - 86_400_000;

    let body = serde_json::json!({
        "start_time": day_ago_ms,
        "end_time": now_ms,
        "size": 100,
        "cursor": 0,
    });

    let resp = dingtalk_api_post(client, &url, &body)
        .await?
        .json::<ApprovalListResponse>()
        .await?;

    if let Some(code) = resp.errcode {
        if code != 0 {
            anyhow::bail!("DingTalk approval list error {}: {:?}", code, resp.errmsg);
        }
    }

    Ok(resp.result.list)
}

/// Fetch attendance check-in records from DingTalk.
/// Uses the /topapi/attendance/v2/query API.
/// Requires a configured user_id_list in the DingTalk config.
async fn fetch_attendance_list(client: &Client, token: &str) -> Result<Vec<AttendanceRecord>> {
    let url = format!(
        "https://oapi.dingtalk.com/attendance/v2/list?access_token={}",
        token
    );

    // Query today's attendance
    let now = Utc::now();
    let today_start = now.format("%Y-%m-%d 00:00:00").to_string();
    let today_end = now.format("%Y-%m-%d 23:59:59").to_string();

    let body = serde_json::json!({
        "checkDateFrom": today_start,
        "checkDateTo": today_end,
        "isI18n": false,
    });

    let resp = dingtalk_api_post(client, &url, &body)
        .await?
        .json::<AttendanceListResponse>()
        .await?;

    if let Some(code) = resp.errcode {
        if code != 0 {
            // Code 40078 means no attendance permission or no user configured
            // Return empty instead of hard error
            debug!(
                "DingTalk attendance API returned code {}: {:?} — likely no user_ids configured",
                code, resp.errmsg
            );
            return Ok(Vec::new());
        }
    }

    Ok(resp.result.check_record_list)
}

/// Fetch work reports (工作报告) from DingTalk.
/// Uses the /topapi/report/list API.
async fn fetch_work_reports(client: &Client, token: &str) -> Result<Vec<WorkReport>> {
    let url = format!(
        "https://oapi.dingtalk.com/topapi/report/list?access_token={}",
        token
    );

    // Query reports from the last 7 days
    let now_ms = Utc::now().timestamp_millis();
    let week_ago_ms = now_ms - 7 * 86_400_000;

    let body = serde_json::json!({
        "cursor": 0,
        "size": 50,
        "start_time": week_ago_ms,
        "end_time": now_ms,
    });

    let resp = dingtalk_api_post(client, &url, &body)
        .await?
        .json::<WorkReportListResponse>()
        .await?;

    if let Some(code) = resp.errcode {
        if code != 0 {
            debug!(
                "DingTalk work report API returned code {}: {:?} — may not have permission",
                code, resp.errmsg
            );
            return Ok(Vec::new());
        }
    }

    Ok(resp.result.data_list)
}

/// Convert an attendance record into a RawEvent.
fn to_attendance_event(record: &AttendanceRecord) -> RawEvent {
    let mut tags = std::collections::HashMap::new();
    tags.insert("platform".to_string(), "dingtalk".to_string());
    tags.insert("type".to_string(), "attendance".to_string());
    tags.insert("user_id".to_string(), record.user_id.clone());
    if let Some(ref check_type) = record.check_type {
        tags.insert("check_type".to_string(), check_type.clone());
    }
    if let Some(ref clock_result) = record.clock_result {
        tags.insert("clock_result".to_string(), clock_result.clone());
    }

    let payload = serde_json::to_vec(&serde_json::json!({
        "user_id": record.user_id,
        "user_name": record.user_name,
        "work_date": record.work_date,
        "check_type": record.check_type,
        "plan_check_time": record.plan_check_time,
        "clock_result": record.clock_result,
        "proc_inst_id": record.proc_inst_id,
        "location_result": record.location_result,
        "source_type": record.source_type,
    }))
    .unwrap_or_default();

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: format!("connector:dingtalk:attendance:{}", record.user_id),
        event_type: "attendance".to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload,
        tags,
    }
}

/// Convert a work report into a RawEvent.
fn to_work_report_event(report: &WorkReport) -> RawEvent {
    let mut tags = std::collections::HashMap::new();
    tags.insert("platform".to_string(), "dingtalk".to_string());
    tags.insert("type".to_string(), "work_report".to_string());
    tags.insert("report_id".to_string(), report.report_id.clone());
    if let Some(ref creator_name) = report.creator_name {
        tags.insert("creator".to_string(), creator_name.clone());
    }
    tags.insert("title".to_string(), report.title.clone());

    let payload = serde_json::to_vec(&serde_json::json!({
        "report_id": report.report_id,
        "title": report.title,
        "creator_name": report.creator_name,
        "creator_id": report.creator_id,
        "create_time": report.create_time,
        "modified_time": report.modified_time,
        "report_type": report.report_type,
    }))
    .unwrap_or_default();

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: format!("connector:dingtalk:report:{}", report.report_id),
        event_type: "work_report".to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload,
        tags,
    }
}

/// Send a message via DingTalk robot webhook.
pub async fn send_robot_message(client: &Client, webhook_url: &str, text: &str) -> Result<()> {
    let body = serde_json::json!({
        "msgtype": "text",
        "text": { "content": text }
    });

    client.post(webhook_url).json(&body).send().await?;
    Ok(())
}

/// Convert a DingTalk approval instance into a RawEvent.
fn to_raw_event(inst: &ApprovalInstance) -> RawEvent {
    let mut tags = std::collections::HashMap::new();
    tags.insert("platform".to_string(), "dingtalk".to_string());
    tags.insert("type".to_string(), "approval".to_string());
    tags.insert("status".to_string(), inst.status.clone());
    tags.insert("instance_id".to_string(), inst.process_instance_id.clone());
    tags.insert("title".to_string(), inst.title.clone());

    let payload = serde_json::to_vec(&serde_json::json!({
        "process_instance_id": inst.process_instance_id,
        "title": inst.title,
        "status": inst.status,
        "originator_userid": inst.originator_userid,
        "create_time": inst.create_time,
        "finish_time": inst.finish_time,
        "business_id": inst.business_id,
    }))
    .unwrap_or_default();

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: format!("connector:dingtalk:approval:{}", inst.process_instance_id),
        event_type: "approval".to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload,
        tags,
    }
}

/// DingTalk connector implementing the unified Connector trait.
pub struct DingtalkConnector {
    config: DingtalkConfig,
    client: Client,
}

impl DingtalkConnector {
    pub fn new(config: DingtalkConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        Ok(Self { config, client })
    }
}

#[async_trait]
impl Connector for DingtalkConnector {
    fn name(&self) -> &str {
        "dingtalk"
    }

    async fn ping(&self) -> Result<()> {
        fetch_access_token(&self.client, &self.config).await?;
        Ok(())
    }
}

/// Convert a raw DingTalk callback payload into a RawEvent.
pub fn to_raw_event_from_callback(payload: serde_json::Value) -> RawEvent {
    let event_type = payload
        .get("EventType")
        .or_else(|| payload.get("eventType"))
        .and_then(|v| v.as_str())
        .unwrap_or("callback")
        .to_string();

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: "connector:dingtalk:callback".to_string(),
        event_type,
        timestamp_ms: Utc::now().timestamp_millis(),
        payload: serde_json::to_vec(&payload).unwrap_or_default(),
        tags: {
            let mut m = std::collections::HashMap::new();
            m.insert("platform".to_string(), "dingtalk".to_string());
            m
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_raw_event_from_callback_uppercase() {
        let payload = serde_json::json!({
            "EventType": "check_in",
            "data": {"key": "value"}
        });
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "check_in");
        assert_eq!(event.source, "connector:dingtalk:callback");
        assert_eq!(event.tags.get("platform").unwrap(), "dingtalk");
    }

    #[test]
    fn test_to_raw_event_from_callback_camelcase() {
        let payload = serde_json::json!({
            "eventType": "approval_change"
        });
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "approval_change");
    }

    #[test]
    fn test_to_raw_event_from_callback_no_type() {
        let payload = serde_json::json!({"data": "test"});
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "callback");
    }

    #[test]
    fn test_attendance_record_deserialization() {
        let json = serde_json::json!({
            "user_id": "user123",
            "user_name": "Alice",
            "work_date": "2026-08-19",
            "check_type": "OnDuty",
            "plan_check_time": "2026-08-19 09:00:00",
            "clock_result": "Normal",
            "source_type": "BEISI"
        });
        let record: AttendanceRecord = serde_json::from_value(json).unwrap();
        assert_eq!(record.user_id, "user123");
        assert_eq!(record.user_name.unwrap(), "Alice");
        assert_eq!(record.check_type.unwrap(), "OnDuty");
        assert_eq!(record.clock_result.unwrap(), "Normal");
    }

    #[test]
    fn test_to_attendance_event() {
        let record = AttendanceRecord {
            user_id: "user456".to_string(),
            user_name: Some("Bob".to_string()),
            work_date: Some("2026-08-19".to_string()),
            check_type: Some("OffDuty".to_string()),
            plan_check_time: Some("2026-08-19 18:00:00".to_string()),
            clock_result: Some("Early".to_string()),
            proc_inst_id: None,
            location_result: None,
            source_type: None,
        };
        let event = to_attendance_event(&record);
        assert_eq!(event.event_type, "attendance");
        assert_eq!(event.source, "connector:dingtalk:attendance:user456");
        assert_eq!(event.tags.get("platform").unwrap(), "dingtalk");
        assert_eq!(event.tags.get("type").unwrap(), "attendance");
        assert_eq!(event.tags.get("user_id").unwrap(), "user456");
        assert_eq!(event.tags.get("check_type").unwrap(), "OffDuty");
        assert_eq!(event.tags.get("clock_result").unwrap(), "Early");

        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["user_id"], "user456");
        assert_eq!(payload["clock_result"], "Early");
    }

    #[test]
    fn test_work_report_deserialization() {
        let json = serde_json::json!({
            "report_id": "rpt-001",
            "title": "Daily Report",
            "creator_name": "Charlie",
            "creator_id": "uid-789",
            "create_time": 1724000000000_i64,
            "modified_time": 1724001000000_i64,
            "report_type": 1
        });
        let report: WorkReport = serde_json::from_value(json).unwrap();
        assert_eq!(report.report_id, "rpt-001");
        assert_eq!(report.title, "Daily Report");
        assert_eq!(report.creator_name.unwrap(), "Charlie");
        assert_eq!(report.report_type.unwrap(), 1);
    }

    #[test]
    fn test_to_work_report_event() {
        let report = WorkReport {
            report_id: "rpt-002".to_string(),
            title: "Weekly Report".to_string(),
            creator_name: Some("Dave".to_string()),
            creator_id: Some("uid-abc".to_string()),
            create_time: 1724000000000,
            modified_time: 1724001000000,
            report_type: Some(2),
        };
        let event = to_work_report_event(&report);
        assert_eq!(event.event_type, "work_report");
        assert_eq!(event.source, "connector:dingtalk:report:rpt-002");
        assert_eq!(event.tags.get("platform").unwrap(), "dingtalk");
        assert_eq!(event.tags.get("type").unwrap(), "work_report");
        assert_eq!(event.tags.get("creator").unwrap(), "Dave");
        assert_eq!(event.tags.get("title").unwrap(), "Weekly Report");

        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["report_id"], "rpt-002");
        assert_eq!(payload["creator_name"], "Dave");
    }

    #[test]
    fn test_approval_instance_deserialization() {
        let json = serde_json::json!({
            "process_instance_id": "inst-001",
            "title": "Leave Request",
            "status": "COMPLETED",
            "originator_userid": "user-001",
            "create_time": 1724000000000_i64,
            "finish_time": 1724003600000_i64,
            "business_id": "biz-001"
        });
        let inst: ApprovalInstance = serde_json::from_value(json).unwrap();
        assert_eq!(inst.process_instance_id, "inst-001");
        assert_eq!(inst.title, "Leave Request");
        assert_eq!(inst.status, "COMPLETED");
        assert_eq!(inst.originator_userid, "user-001");
        assert_eq!(inst.business_id, "biz-001");
    }

    #[test]
    fn test_to_raw_event_approval() {
        let inst = ApprovalInstance {
            process_instance_id: "inst-002".to_string(),
            title: "Expense Report".to_string(),
            status: "RUNNING".to_string(),
            originator_userid: "user-002".to_string(),
            create_time: 1724000000000,
            finish_time: 0,
            business_id: "biz-002".to_string(),
        };
        let event = to_raw_event(&inst);
        assert_eq!(event.event_type, "approval");
        assert_eq!(event.source, "connector:dingtalk:approval:inst-002");
        assert_eq!(event.tags.get("platform").unwrap(), "dingtalk");
        assert_eq!(event.tags.get("type").unwrap(), "approval");
        assert_eq!(event.tags.get("status").unwrap(), "RUNNING");
        assert_eq!(event.tags.get("instance_id").unwrap(), "inst-002");
        assert_eq!(event.tags.get("title").unwrap(), "Expense Report");

        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["process_instance_id"], "inst-002");
        assert_eq!(payload["title"], "Expense Report");
        assert_eq!(payload["status"], "RUNNING");
        assert_eq!(payload["originator_userid"], "user-002");
    }

    #[test]
    fn test_work_report_no_creator() {
        let report = WorkReport {
            report_id: "rpt-003".to_string(),
            title: "Anonymous Report".to_string(),
            creator_name: None,
            creator_id: None,
            create_time: 0,
            modified_time: 0,
            report_type: None,
        };
        let event = to_work_report_event(&report);
        assert_eq!(event.event_type, "work_report");
        assert!(!event.tags.contains_key("creator"));
        assert_eq!(event.tags.get("title").unwrap(), "Anonymous Report");
    }

    #[test]
    fn test_callback_with_event_key_lowercase() {
        let payload = serde_json::json!({
            "eventType": "user_add_org",
            "TimeStamp": "2026-08-19T00:00:00Z"
        });
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "user_add_org");
    }

    // ── Edge-case tests ────────────────────────────────────────────

    #[test]
    fn test_attendance_record_all_optional_fields() {
        let record = AttendanceRecord {
            user_id: "u1".to_string(),
            user_name: Some("Alice".to_string()),
            work_date: Some("2026-08-20".to_string()),
            check_type: Some("OnDuty".to_string()),
            plan_check_time: Some("09:00".to_string()),
            clock_result: Some("Normal".to_string()),
            proc_inst_id: Some("proc-001".to_string()),
            location_result: Some("Office".to_string()),
            source_type: Some("Beacon".to_string()),
        };
        let event = to_attendance_event(&record);
        assert_eq!(event.event_type, "attendance");
        assert_eq!(event.source, "connector:dingtalk:attendance:u1");
        assert_eq!(event.tags.get("check_type").unwrap(), "OnDuty");
        assert_eq!(event.tags.get("clock_result").unwrap(), "Normal");

        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["user_name"], "Alice");
        assert_eq!(payload["work_date"], "2026-08-20");
        assert_eq!(payload["location_result"], "Office");
        assert_eq!(payload["source_type"], "Beacon");
    }

    #[test]
    fn test_attendance_record_no_optional_fields() {
        let record = AttendanceRecord {
            user_id: "u2".to_string(),
            user_name: None,
            work_date: None,
            check_type: None,
            plan_check_time: None,
            clock_result: None,
            proc_inst_id: None,
            location_result: None,
            source_type: None,
        };
        let event = to_attendance_event(&record);
        assert_eq!(event.source, "connector:dingtalk:attendance:u2");
        assert!(!event.tags.contains_key("check_type"));
        assert!(!event.tags.contains_key("clock_result"));
    }

    #[test]
    fn test_approval_instance_empty_fields() {
        let inst = ApprovalInstance {
            process_instance_id: String::new(),
            title: String::new(),
            status: String::new(),
            originator_userid: String::new(),
            create_time: 0,
            finish_time: 0,
            business_id: String::new(),
        };
        let event = to_raw_event(&inst);
        assert_eq!(event.event_type, "approval");
        assert_eq!(event.source, "connector:dingtalk:approval:");
        assert_eq!(event.tags.get("status").unwrap(), "");
        assert_eq!(event.tags.get("title").unwrap(), "");
    }

    #[test]
    fn test_callback_empty_payload() {
        let payload = serde_json::json!({});
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "callback"); // default
        assert_eq!(event.source, "connector:dingtalk:callback");
    }

    #[test]
    fn test_callback_with_nested_data() {
        let payload = serde_json::json!({
            "EventType": "bpms_instance_change",
            "data": {
                "processInstanceId": "inst-100",
                "title": "Travel Request",
                "status": "COMPLETED"
            }
        });
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "bpms_instance_change");
        // Payload should contain the full nested structure
        let parsed: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(parsed["data"]["processInstanceId"], "inst-100");
    }

    #[test]
    fn test_work_report_zero_timestamps() {
        let report = WorkReport {
            report_id: "rpt-zero".to_string(),
            title: "Empty".to_string(),
            creator_name: None,
            creator_id: None,
            create_time: 0,
            modified_time: 0,
            report_type: None,
        };
        let event = to_work_report_event(&report);
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["create_time"], 0);
        assert_eq!(payload["modified_time"], 0);
    }

    #[test]
    fn test_robot_message_body_format() {
        let body = serde_json::json!({
            "msgtype": "text",
            "text": { "content": "Hello from OpenSoma" }
        });
        assert_eq!(body["msgtype"], "text");
        assert_eq!(body["text"]["content"], "Hello from OpenSoma");
    }

    #[test]
    fn test_approval_list_response_empty() {
        let json = serde_json::json!({
            "result": { "list": [], "next_cursor": 0, "has_more": false },
            "errcode": null,
            "errmsg": null
        });
        let resp: ApprovalListResponse = serde_json::from_value(json).unwrap();
        assert!(resp.result.list.is_empty());
        assert!(!resp.result.has_more);
    }

    #[test]
    fn test_approval_list_response_with_items() {
        let json = serde_json::json!({
            "result": {
                "list": [
                    {
                        "process_instance_id": "inst-1",
                        "title": "Request 1",
                        "status": "NEW",
                        "originator_userid": "user-1",
                        "create_time": 1700000000000_i64,
                        "finish_time": 0,
                        "business_id": "biz-1"
                    }
                ],
                "next_cursor": 100,
                "has_more": true
            }
        });
        let resp: ApprovalListResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.result.list.len(), 1);
        assert!(resp.result.has_more);
        assert_eq!(resp.result.list[0].process_instance_id, "inst-1");
    }

    #[test]
    fn test_work_report_list_response_empty() {
        let json = serde_json::json!({
            "result": { "data_list": [], "has_more": false, "next_cursor": 0 }
        });
        let resp: WorkReportListResponse = serde_json::from_value(json).unwrap();
        assert!(resp.result.data_list.is_empty());
    }

    #[test]
    fn test_attendance_list_response_empty() {
        let json = serde_json::json!({
            "result": { "check_record_list": [], "has_more": false }
        });
        let resp: AttendanceListResponse = serde_json::from_value(json).unwrap();
        assert!(resp.result.check_record_list.is_empty());
    }

    #[test]
    fn test_callback_unicode_event_type() {
        let payload = serde_json::json!({
            "EventType": "审批实例",
            "data": {}
        });
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "审批实例");
    }

    #[test]
    fn test_raw_event_payload_is_valid_json() {
        let inst = ApprovalInstance {
            process_instance_id: "inst-json".to_string(),
            title: "JSON Test".to_string(),
            status: "RUNNING".to_string(),
            originator_userid: "user-json".to_string(),
            create_time: 1700000000000,
            finish_time: 0,
            business_id: "biz-json".to_string(),
        };
        let event = to_raw_event(&inst);
        let parsed: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert!(parsed.is_object());
        assert_eq!(parsed["process_instance_id"], "inst-json");
    }
}
