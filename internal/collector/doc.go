// Package collector lists and watches the configured resource types and
// writes every observed change, plus coverage boundaries, to a
// store.Store, following docs/adr/0002–0005.
//
// # Structure
//
// One cache.Reflector per resource type, with no informer and no object
// cache. Each reflector is given:
//
//   - a ListerWatcher wrapper (sourceLW) that proxies every watch stream,
//     so the collector knows when a stream ended cleanly, and that can
//     force a relist (close the stream, fail the next Watch with 410);
//   - a custom store (sourceStore) that normalizes objects (package
//     normalize) and puts them on a bounded queue.
//
// A single writer goroutine drains the queue, does change detection
// against a small per-key state (UID + content hash; no objects kept in
// memory), batches store.Entry values and owns all coverage decisions.
// One writer gives one commit order, matching the store's Seq.
//
// # Batches and ordering
//
// Initial LIST of a resource with no coverage history:
//
//	[baseline Added...]  [Start]
//
// Relist after continuity was lost, or first LIST after a restart:
//
//	[End(lostAt)]?  [resync Added/Modified/Deleted...]  [Start]
//
// Every End is placed strictly after the resource's latest record
// (resource.endAt): the query engine treats End.At <= T as the End, so an
// End at exactly the last record's time would turn that observed record
// into gap.
//
// End is present only if coverage is open in the store. It goes first
// because the store requires End.At >= the resource's latest record time,
// and the resync records are stamped with the relist time (later than
// lostAt). Start goes last; the store clamps it after every record of the
// batch (and, since main e04b7f8, after the preceding End), so "observed
// from Start.At" means the LIST is completely applied. A key whose UID
// changed yields resync Deleted(old UID, last stored object) then Added.
// Batches larger than Config.MaxBatchSize are split with End in the first
// chunk and Start in the last; a crash between chunks leaves coverage
// closed, and the next LIST is diffed against whatever was written.
//
// lostAt is the last instant the source knows it saw everything: the
// receipt of the last accepted watch event, the last bookmark, or the last
// applied LIST. It is frozen while a LIST is in progress. A stream that
// closes without an error does not advance it: the apiserver may drop
// events buffered for a watcher when it terminates the watch, so only
// delivered events and bookmarks prove progress. For a quiet resource
// that costs at most a bookmark interval (~1 min) of coverage when a
// resume then fails.
//
// Reliability D3 (accepted, no change): because of this, an idle
// resource whose watch is lost ends coverage at its last event, bookmark
// or LIST, possibly long before the watch actually broke, and an
// interval can have zero length (End.At == Start.At, the instant of its
// LIST). That shows a longer gap than strictly necessary, which is the
// safe direction under ADR 0004; it is not fabricated continuity.
//
// Watch events become Records with origin=watch, ObservedAt from the
// injected clock at receipt. An update whose normalized hash is unchanged
// is skipped (debug log). Delete records carry the final state delivered
// by the watch; for an object-less cache.DeletedFinalStateUnknown, the
// last stored state.
//
// UID mismatches on a watch (impossible on a truly continuous watch) are
// recorded as what must have happened, with the inferred part marked
// resync: an Added/Modified for a new UID yields Deleted(old UID, last
// stored state, origin resync) then Added/watch; a Deleted for a UID other
// than the known one yields only Deleted(known UID, last stored state,
// origin resync), the never-seen instance is not recorded, and the key is
// absent afterwards.
//
// # Time stamping and heartbeats
//
// ObservedAt is assigned at receipt, in the reflector callback, before it
// waits for queue space; the source marks itself "waiting" under the same
// lock. Stamping at acceptance (the earlier design) made every change late
// by the length of a store stall while coverage still said observed.
//
// Receipt stamping alone does not bound the error, because events still
// unread in the watch stream are stamped only when the blocked reflector
// gets to them. Hence the stall rule: if an item waits longer than
// Config.StallTimeout (default 5s) for queue space, the source forces a
// relist, the item is still recorded (its stamp is accurate), and the
// writer ends coverage at that item's receipt time (reason "stall"). The
// changes the reflector had not read yet are re-derived by the relist as
// resync records inside the gap. So no ObservedAt inside observed coverage
// is late by more than StallTimeout. Short waits (under StallTimeout) are
// ordinary batching backpressure and keep coverage.
//
// Within a resource, queue order is receipt order (one reflector
// goroutine). Across resources a waiting item can be overtaken by a
// later-stamped one; the store's clamping makes such times non-decreasing
// (by at most the wait, itself bounded by StallTimeout).
//
// A heartbeat is a queue item stamped before the sources are checked. It
// is enqueued only if no source has a received item still waiting and all
// are synced with a live watch, so every item received before the
// heartbeat's time is ahead of it in the queue; the writer calls
// store.SetHeartbeat only after flushing them. After a crash End =
// max(heartbeat, latest record, latest Start) therefore never claims
// coverage over a change that was received but not written.
//
// # Backpressure and write failures
//
// The queue is bounded (Config.QueueSize). When the store is slow the
// writer is busy, the queue fills, and the reflector goroutine blocks
// inside Add/Update/Replace: it stops reading its watch stream, and the
// API server buffers or eventually drops the watch (which then resumes
// or relists, both handled). Memory does not grow. A wait longer than
// StallTimeout ends coverage (see above).
//
// A failing write is retried with exponential backoff for
// Config.WriteRetryTimeout. After that, the batch is dropped, but not
// silently: every resource in it is marked lost with lostAt just before
// its first unwritten change, its later watch events are discarded, and
// a relist is forced. The relist batch writes End(lostAt,
// "write-failure"), resyncs against what the store really holds
// (change-detection state is reloaded from the store), and Starts again.
// ErrInvalid is not retried; ErrClosed stops the collector.
//
// # Process stalls (ADR 0007 §1)
//
// A watchdog ticks every Config.WatchdogInterval (1s) on the injected
// clock, and every receipt in a source reports its time to it as well. If
// two consecutive observations are more than StallTimeout apart, the
// process did not run in between (SIGSTOP, VM or container freeze, CPU
// starvation), and anything received from then on may carry a late
// stamp. Every source is marked stalled and relisted, and a stall item is
// queued under the watchdog's lock, so no receipt that observes after the
// trip can be queued ahead of it. The writer drops pending records stamped
// after the last on-time observation and marks every resource lost there.
// The relist writes End(last on-time observation, "process-stall"),
// resyncs, and Starts.
//
// Receipts report to the watchdog because the first event delivered after
// a freeze then trips it before that event is queued; the ticker alone
// could let the backlog through first. If records stamped after the last
// on-time observation were nevertheless already written (an item queued
// between resume and the trip and flushed in a full batch), the store
// requires End after them; resource.endAt places it there and the writer
// logs it. The watchdog cannot see a slow API server delivering late
// while this process runs normally (ADR 0002 delivery lag).
//
// A forced relist (write failure, queue stall, process stall) is
// satisfied only by a LIST that started after the request (a generation
// counter, not a time, so fake clocks cannot tie). A LIST already in
// progress or queued when the watchdog tripped does not clear it.
//
// # Live coverage loss (ADR 0007 §2)
//
// ResourceStatus.LostAt reports continuity lost in memory but not yet
// durable as an End: the earliest of a source-side loss (queue stall,
// process stall; known even while the writer is stuck in a failing
// Write, the disk-full case) and a writer-side loss (abandoned write,
// failed list, process stall). It is cleared when an End at or after it
// commits, or when a later LIST is applied, and is nil whenever coverage
// is not open in the store. The query engine overlays it as gap. The last
// store write error and its time are reported per resource and kept.
//
// # Startup and shutdown
//
// At startup the latest record of every key is loaded (View.List at a far
// future time) and its hash recomputed from the stored bytes
// (normalize.NormalizeJSON). The resource's latest record time comes from
// View.LatestRecordTime, which compaction never lowers (List no longer
// shows compacted delete records, and an End before them is rejected).
// If a resource's latest coverage event is a Start (no clean End: crash),
// End(max(heartbeat, latest record, latest Start), "crash-recovery") is
// written before anything else.
//
// On context cancellation each source records whether it still had
// continuity, and only then are the reflectors stopped (they run on their
// own context, not one derived from Run's, so they cannot tear their
// watches down before that snapshot). A shutdown item stamped
// after they stopped is queued; the writer flushes everything before it
// and writes End("shutdown") for every open resource. The End is at the
// shutdown time only for a source that was healthy when shutdown began
// and dropped nothing while stopping; otherwise (watch lost, relist in
// progress or failing, stalled, or a received item dropped because the
// queue was full) at the source's continuity bound, lostAt above.
//
// The final writes are bounded by Config.ShutdownTimeout. If a store Write
// ignores cancellation and does not return, Run returns an error after the
// timeout with the writer goroutine still inside Write; the caller then
// closes the store, and Store.Close must make that Write return. The next
// start treats the open coverage as a crash.
//
// # Research notes (client-go / apimachinery v0.37.2)
//
// Each claim is VERIFIED against the module-cache source (file:function)
// or marked ASSUMPTION.
//
// When Store.Replace is called:
//
//   - VERIFIED tools/cache/reflector.go:RunWithContext runs
//     ListAndWatchWithContext in a backoff loop (wait.Until-style
//     delayHandler, default 800ms doubling to 30s, jittered).
//     ListAndWatchWithContext always starts with a list (list or
//     watchList), so every ListAndWatch iteration calls Replace exactly
//     once before watching. Every Replace after the first in a process is
//     therefore a relist after the previous watch could not be continued.
//   - VERIFIED reflector.go:list calls syncWith -> store.Replace with the
//     full item slice (pager, possibly paginated, accumulated in memory).
//     If the LIST at the last RV returns Expired/too-large-RV it retries at
//     RV="" (quorum read) in the same call.
//   - VERIFIED reflector.go:watch resumes from LastSyncResourceVersion
//     without relisting when: the stream closes normally (handleAnyWatch
//     returns nil), the Watch call fails with a retriable error
//     (isWatchErrorRetriable: connection refused, 429), an error event is
//     429 (backoff, continue), or an InternalError within
//     MaxInternalErrorRetryDuration.
//   - VERIFIED reflector.go:watch returns nil (so ListAndWatch returns and
//     the next iteration relists) on an error event that is Expired/Gone
//     (410), any other error event, or VeryShortWatchError (stream closed
//     in < 1s with no events, handleAnyWatch). It returns the error itself
//     when the Watch call fails with a non-retriable error (e.g. 410, 403).
//   - VERIFIED reflector.go:RunWithContext calls the WatchErrorHandler only
//     when ListAndWatchWithContext returns a non-nil error: a failed list,
//     a failed Replace, or a non-retriable Watch-call error. It is NOT
//     called for a 410 delivered as a watch event (watch returns nil).
//     The handler is an unexported field settable only through
//     controller Config / sharedIndexInformer.SetWatchErrorHandler, not on
//     a bare Reflector. So the collector does not rely on it.
//   - VERIFIED reflector.go:handleAnyWatch only logs errors returned by
//     store Add/Update/Delete (utilruntime.HandleErrorWithContext, which
//     would also log the object) and continues: a store that returns an
//     error loses the event silently. sourceStore never returns one.
//   - VERIFIED bookmarks: handleAnyWatch passes Bookmark events to a store
//     implementing ReflectorBookmarkStore and updates the RV; no Replace.
//     reflector.go:watch always sets AllowWatchBookmarks. The apiserver
//     (k8s.io/apiserver v0.36.0 pkg/storage/cacher/cache_watcher.go:
//     nextBookmarkTime, cacher.go defaultBookmarkFrequency = 1m) sends one
//     about every minute and ~2s before the watch deadline.
//   - VERIFIED WatchList: features/known_features.go WatchListClient is
//     Beta and default true from 1.35. NewReflectorWithOptions disables it
//     if the ListerWatcher implements IsWatchListSemanticsUnSupported()
//     returning true (util/watchlist.DoesClientNotSupportWatchListSemantics;
//     the fake dynamic/metadata clients do). reflector.go:watchList opens a
//     watch with SendInitialEvents=true, feeds initial events into a
//     temporary cache.Store (not ours), and on the bookmark annotated
//     k8s.io/initial-events-end calls store.Replace exactly once with the
//     temporary store's contents, then keeps the same stream as the live
//     watch. If streaming fails non-retriably it falls back to list.
//   - VERIFIED apimachinery pkg/watch/streamwatcher.go:receive closes the
//     result channel without an error event on io.EOF, io.ErrUnexpectedEOF,
//     probable-EOF and timeout errors; other decode errors become an Error
//     event. A dropped connection is therefore indistinguishable from a
//     normal server-side close at the watch.Interface level.
//
// Detecting "a relist happened after continuity was lost":
//
//   - VERIFIED (from the above) a Replace that is not the first in the
//     process always follows a broken watch, so it always closes a gap;
//     the collector also keys on durable state: if the store says
//     coverage is open when a Replace is applied, it writes End first.
//     That covers the first Replace after a restart too (coverage was
//     closed by shutdown or crash recovery, so no End is needed).
//   - The gap's start cannot be read from the reflector (no exported hook
//     for "stream ended"). The collector does not assume a closed stream
//     delivered everything up to the close (the review showed the
//     apiserver may drop a terminated watcher's buffered events); the gap
//     starts at the last delivered event or bookmark.
//   - ASSUMPTION: between a successful Watch call and a 410 error event at
//     the start of that stream (milliseconds) a heartbeat could be
//     written; a crash in that window would claim that much extra
//     coverage.
//   - ASSUMPTION: events the apiserver committed but had not yet sent
//     when a change was received are attributed by delivery order;
//     ObservedAt is accurate to delivery lag (ADR 0002).
//
// Metadata-only list/watch (k8s.io/client-go/metadata):
//
//   - VERIFIED metadata/metadata.go:client.List/Watch request
//     "Accept: application/vnd.kubernetes.protobuf;as=PartialObjectMetadata(List);
//     g=meta.k8s.io;v=v1,application/json;as=...", so the server converts
//     to PartialObjectMetadata before sending: Secret data never reaches
//     the process. List falls back to decoding raw JSON into
//     PartialObjectMetadataList if the type is not registered.
//   - VERIFIED apimachinery pkg/apis/meta/v1/types.go: PartialObjectMetadata
//     is TypeMeta + ObjectMeta only (name, namespace, uid, resourceVersion,
//     generation, creationTimestamp, deletionTimestamp, labels,
//     annotations, ownerReferences, finalizers, managedFields).
//     Annotations can include kubectl's last-applied-configuration with the
//     full Secret, or tokens (e.g. openshift.io/token-secret.value), so
//     normalize keeps only allow-listed annotations on metadata-only types
//     (default none); managedFields name the data keys, so normalize always
//     drops them for metadata-only types.
//   - VERIFIED by the integration test against kind (server v1.37.0):
//     items of a metadata LIST carry an empty TypeMeta (not v1/Secret), so
//     normalize always sets apiVersion/kind from the resource type.
//
// Memory, Reflector + custom store vs SharedInformer, 10k+ objects:
//
//   - VERIFIED shared_informer.go:NewSharedIndexInformerWithOptions keeps
//     every object in an Indexer (thread-safe map of full decoded
//     objects) for the informer's lifetime, and controller.go feeds it
//     through a DeltaFIFO/RealFIFO that holds deltas (full objects) until
//     processed. For 10k Pods at ~5–20 KiB decoded that is on the order of
//     100 MiB+ resident, permanently (ASSUMPTION for the per-object size).
//   - VERIFIED a bare Reflector keeps nothing itself besides the RV; the
//     full LIST exists transiently (reflector.go:list via
//     meta.ExtractListWithAlloc, or watchList's temporary store) until
//     Replace returns. This collector then holds the normalized bytes of
//     that LIST until written (one queue item), and permanently only
//     ~100–200 bytes per live object (key, UID, 32-byte hash). Peak memory
//     is about one LIST of the largest resource, not the sum of all
//     objects of all resources.
package collector
