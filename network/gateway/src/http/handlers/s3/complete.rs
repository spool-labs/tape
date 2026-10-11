//! CompleteMultipartUpload's detached write and the 200 that waits on it
//!
//! S3 answers a Complete with 200 at once, keeps the connection open with
//! whitespace while it writes, and puts the result or an `<Error>` in the same
//! body. The write runs in its own task, so a client that hangs up cancels
//! nothing, and a retried Complete joins the running write or gets the answer
//! of the one that landed.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures::Stream;
use futures::stream;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use tape_crypto::address::Address;

use super::error::S3Error;
use super::xml::XML_DECL;

/// Time between the spaces a waiting Complete sends to keep its connection open
const KEEPALIVE: Duration = Duration::from_secs(10);

/// How long a landed Complete's answer stays for a retry of the same upload
const LANDED_TTL: Duration = Duration::from_secs(60 * 60);

/// A Complete's answer, the result or `<Error>` element, once its write ends
pub type Outcome = watch::Receiver<Option<Arc<str>>>;

/// The sending side of an [`Outcome`]
pub type Answer = watch::Sender<Option<Arc<str>>>;

/// Completes running or recently landed, by upload id
static COMPLETIONS: LazyLock<Mutex<HashMap<String, Completion>>> =
    LazyLock::new(Default::default);

/// One registered Complete
struct Completion {
    bucket: Address,
    key: String,
    principal: Address,
    outcome: Outcome,
    /// When the write landed, once it has
    landed_at: Option<Instant>,
    /// The write task, kept so it stays joinable
    task: Option<JoinHandle<()>>,
}

impl Completion {
    /// Whether this entry still answers a retry: a write that runs, or one
    /// that landed within the TTL
    fn is_live(&self, now: Instant) -> bool {
        match self.landed_at {
            Some(landed_at) => now.duration_since(landed_at) < LANDED_TTL,
            // A write task that ended without an answer leaves a closed channel.
            None => self.outcome.has_changed().is_ok(),
        }
    }
}

/// What registering a Complete found
pub enum Registration {
    /// No Complete was registered, so this one writes and answers through the sender
    Started(Answer),
    /// A Complete for this upload runs or landed, so this one waits on its answer
    Joined(Outcome),
}

/// The answer of a Complete for this upload that runs or landed, when the
/// caller owns the upload
pub fn attach(upload_id: &str, bucket: Address, key: &str, principal: Address) -> Option<Outcome> {
    let mut completions = lock();
    prune(&mut completions);
    let completion = completions.get(upload_id)?;
    let is_owner =
        completion.bucket == bucket && completion.key == key && completion.principal == principal;
    is_owner.then(|| completion.outcome.clone())
}

/// Register a Complete for this upload, or join the one registered first
pub fn register(upload_id: &str, bucket: Address, key: &str, principal: Address) -> Registration {
    let mut completions = lock();
    prune(&mut completions);
    if let Some(completion) = completions.get(upload_id) {
        return Registration::Joined(completion.outcome.clone());
    }
    let (answer, outcome) = watch::channel(None);
    let completion = Completion {
        bucket,
        key: key.to_string(),
        principal,
        outcome,
        landed_at: None,
        task: None,
    };
    completions.insert(upload_id.to_string(), completion);
    Registration::Started(answer)
}

/// Keep a registered Complete's write task
pub fn track(upload_id: &str, task: JoinHandle<()>) {
    if let Some(completion) = lock().get_mut(upload_id) {
        completion.task = Some(task);
    }
}

/// Publish a Complete's answer. A landed write keeps it for retries, and a
/// failed one leaves the upload to a fresh Complete.
pub fn finish(upload_id: &str, answer: Answer, element: String, is_landed: bool) {
    answer.send_replace(Some(Arc::from(element)));
    let mut completions = lock();
    match is_landed {
        true => {
            if let Some(completion) = completions.get_mut(upload_id) {
                completion.landed_at = Some(Instant::now());
            }
        }
        false => {
            completions.remove(upload_id);
        }
    }
}

/// A 200 that sends the XML declaration, a space every `KEEPALIVE` while the
/// write runs, and then its answer
pub fn waiting_response(outcome: Outcome) -> Response {
    let body = Body::from_stream(answer_stream(outcome, KEEPALIVE));
    (StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

/// Where a waiting response is
enum Phase {
    Declaration,
    Waiting,
    Answered,
}

/// The body of a waiting response
fn answer_stream(
    outcome: Outcome,
    keepalive: Duration,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    stream::unfold((Phase::Declaration, outcome), move |(phase, mut outcome)| async move {
        match phase {
            Phase::Declaration => {
                Some((Ok(Bytes::from_static(XML_DECL.as_bytes())), (Phase::Waiting, outcome)))
            }
            Phase::Waiting => {
                let (bytes, phase) = wait_answer(&mut outcome, keepalive).await;
                Some((Ok(bytes), (phase, outcome)))
            }
            Phase::Answered => None,
        }
    })
}

/// Wait up to `keepalive` for the answer: a space while it is not in, then the answer
async fn wait_answer(outcome: &mut Outcome, keepalive: Duration) -> (Bytes, Phase) {
    let element = match timeout(keepalive, outcome.wait_for(Option::is_some)).await {
        Err(_elapsed) => return (Bytes::from_static(b" "), Phase::Waiting),
        Ok(Ok(answer)) => answer.as_deref().map(str::to_string),
        Ok(Err(_closed)) => None,
    };
    let element = element.unwrap_or_else(|| {
        let lost = S3Error::Internal("the Complete write ended without an answer".into());
        lost.element("")
    });
    (Bytes::from(element), Phase::Answered)
}

/// Drop landed answers past their TTL and writes that ended without one
fn prune(completions: &mut HashMap<String, Completion>) {
    let now = Instant::now();
    completions.retain(|_, completion| completion.is_live(now));
}

fn lock() -> MutexGuard<'static, HashMap<String, Completion>> {
    COMPLETIONS.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    fn fresh_id() -> String {
        Address::new_unique().to_string()
    }

    // the body sends the declaration, spaces while waiting, then the answer
    #[tokio::test]
    async fn answer_order() {
        let (answer, outcome) = watch::channel(None);
        let mut body = Box::pin(answer_stream(outcome, Duration::from_millis(20)));

        let declaration = body.next().await.expect("declaration").expect("bytes");
        assert_eq!(declaration, XML_DECL.as_bytes());
        let space = body.next().await.expect("keepalive").expect("bytes");
        assert_eq!(space, " ".as_bytes());

        answer.send_replace(Some(Arc::from("<Done/>")));
        let mut rest = Vec::new();
        while let Some(chunk) = body.next().await {
            rest.extend_from_slice(&chunk.expect("bytes"));
        }
        assert!(rest.ends_with(b"<Done/>"));
        assert!(rest[..rest.len() - b"<Done/>".len()].iter().all(|byte| *byte == b' '));
    }

    // a write task that ends without an answer gives an error element
    #[tokio::test]
    async fn answer_lost() {
        let (answer, outcome) = watch::channel(None);
        drop(answer);
        let body: Vec<Bytes> = answer_stream(outcome, Duration::from_millis(20))
            .map(|chunk| chunk.expect("bytes"))
            .collect()
            .await;
        assert_eq!(body.len(), 2);
        assert!(body[1].starts_with(b"<Error><Code>InternalError</Code>"));
    }

    // a second Complete joins the first, a landed answer stays, a failed one goes
    #[tokio::test]
    async fn retry_joins() {
        let upload_id = fresh_id();
        let bucket = Address::new_unique();
        let owner = Address::new_unique();
        let answer = match register(&upload_id, bucket, "obj", owner) {
            Registration::Started(answer) => answer,
            Registration::Joined(_) => panic!("first Complete should start"),
        };
        let mut joined = match register(&upload_id, bucket, "obj", owner) {
            Registration::Joined(outcome) => outcome,
            Registration::Started(_) => panic!("second Complete should join"),
        };
        assert!(attach(&upload_id, bucket, "obj", Address::new_unique()).is_none());

        finish(&upload_id, answer, "<Result/>".into(), true);
        let landed = joined.wait_for(Option::is_some).await.expect("answer").clone();
        assert_eq!(landed.as_deref(), Some("<Result/>"));
        let retried = attach(&upload_id, bucket, "obj", owner).expect("landed answer");
        assert_eq!(retried.borrow().as_deref(), Some("<Result/>"));

        let failed_id = fresh_id();
        let answer = match register(&failed_id, bucket, "obj", owner) {
            Registration::Started(answer) => answer,
            Registration::Joined(_) => panic!("fresh upload should start"),
        };
        finish(&failed_id, answer, "<Error/>".into(), false);
        assert!(attach(&failed_id, bucket, "obj", owner).is_none());
        assert!(matches!(register(&failed_id, bucket, "obj", owner), Registration::Started(_)));
    }
}
