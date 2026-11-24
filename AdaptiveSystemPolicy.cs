using System;
using System.Collections.Generic;
using System.Linq;
using SPS.Abstractions;

namespace SPS.Student;

public sealed class AdaptiveSystemPolicy : ISystemPolicy
{
    // ===== Processing budgets (per-request lifetime cap) =====
    private static readonly TimeSpan MetadataBudget = TimeSpan.FromSeconds(1.2);
    private static readonly TimeSpan PreviewBudget = TimeSpan.FromSeconds(3.2);
    private static readonly TimeSpan GenerateBudget = TimeSpan.FromSeconds(30.0);

    // ===== Cache tuning =====
    private static readonly TimeSpan SmallEntryTtl = TimeSpan.FromSeconds(2.0);
    private static readonly TimeSpan LargeEntryTtl = TimeSpan.FromSeconds(8.0);
    private const long SmallEntrySizeThreshold = 4096;

    private static readonly TimeSpan FreshnessGuard = TimeSpan.FromMilliseconds(50);

    // ===== Endpoint health review tuning =====
    private const int RecentResultsWindow = 8;
    private const int FailuresForProbe = 3;
    private static readonly TimeSpan StaleHeartbeatLimit = TimeSpan.FromMilliseconds(1500);

    // ===== Job recovery tuning =====
    private static readonly TimeSpan JobHeartbeatStaleLimit = TimeSpan.FromSeconds(2.0);
    private static readonly TimeSpan JobNoHeartbeatLimit = TimeSpan.FromSeconds(8.0);

    private static readonly TimeSpan JobGiveUpAfter = TimeSpan.FromSeconds(25.0);
    private static readonly TimeSpan JobRequeueDelay = TimeSpan.FromMilliseconds(250);

    private const double DoomWorkFactor = 1.35;
    private const double DoomGuardMarginSeconds = 0.5;

    // ===== Capacity plan tuning =====
    private const int BaselineApiCount = 2;
    private const int BaselineWorkerCount = 2;
    private const double WorkersPerUrgentJob = 3.0;
    private const double UrgencyHorizonSeconds = 12.0;
    private const double ProbeHysteresis = 1.0;
    private const double LoadEmaAlpha = 0.3; // weight of the newest sample; half-life ~2 reviews
    private double _loadEma;
    private const double CompletionsEmaAlpha = 0.15;  // per ~1s capacity review
    private const double AvgWorkEmaAlpha = 0.2;

    private const double NoDrainProbeGuard = 0.05;
    private const int GrowConfirmReviews = 3;
    private double _completionsEma;
    private double _avgWorkSeconds = 3.5; // typical Generate work until real estimates arrive
    private int _completionsSinceLastReview;
    private TimeSpan? _lastCapacityReview;
    private int _stabilizedFleet = BaselineWorkerCount;
    private int _growStreak;

    public IRequestPolicy RpcPolicy { get; } = new AdaptiveRequestPolicy();

    // -------- Optional processing budget --------
    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random) => request.Kind switch
    {
        RequestKind.Metadata => MetadataBudget,
        RequestKind.Preview => PreviewBudget,
        RequestKind.Generate => GenerateBudget,
        _ => null
    };

    // -------- Routing: least-loaded eligible API --------
    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        if (context.Endpoints.Count == 0) return null; // wait for the next review

        EndpointInfo? best = null;
        double bestScore = double.MaxValue;
        foreach (var ep in context.Endpoints)
        {
            if (ep.State != InstanceState.Ready || ep.Mode != EndpointMode.AllowTraffic) continue;
            double load = (double)ep.OutstandingCalls / Math.Max(1, ep.Slots);
            double score = load * 10.0 - ep.Speed;
            if (score < bestScore)
            {
                bestScore = score;
                best = ep;
            }
        }
        return best == null ? null : EndpointSelectionDecision.Assign(best.Id);
    }

    // -------- Endpoint health review: probe-divert model --------
    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        var ep = context.Endpoint;
        if (ep.State is InstanceState.Stopped or InstanceState.Starting)
            return EndpointDecision.Allow();

        if (ep.ProbeOutstanding)
            return EndpointDecision.Allow();

        var results = context.RecentResults;
        if (results.Count > 0)
        {
            var last = results[results.Count - 1];
            if (last.IsProbe)
                return last.Outcome == RequestOutcome.Success
                    ? EndpointDecision.Allow()
                    : EndpointDecision.Probe();
        }

        bool heartbeatStale = ep.Role == InstanceRole.Worker
            && ep.AssignedJobs > 0
            && ep.LastHeartbeat.HasValue
            && (context.Now - ep.LastHeartbeat.Value) > StaleHeartbeatLimit;
        if (heartbeatStale)
            return EndpointDecision.Probe();

        int recentFailures = 0, examined = 0;
        for (int i = results.Count - 1; i >= 0 && examined < RecentResultsWindow; i--, examined++)
        {
            var r = results[i];
            if (r.IsProbe && r.Outcome == RequestOutcome.Success) break;
            if (r.Outcome != RequestOutcome.Success && ++recentFailures >= FailuresForProbe)
                return EndpointDecision.Probe();
        }

        return EndpointDecision.Allow();
    }

    // -------- Cache read: serve fresh / join fetch / start fetch --------
    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        var entry = context.Entry;
        var req = context.Request;

        // 1) Cached entry exists and is still within TTL...
        if (entry != null && entry.ExpiresAt > context.Now)
        {
            var age = context.Now - entry.SourceObservedAt;
            if (age + FreshnessGuard <= req.MaxDataAge)
                return CacheReadDecision.ServeCached();
        }

        // 2) Coalesce: if there is already an in-flight fetch for this key, join it instead of
        if (context.PendingFetches.Count > 0)
        {
            FetchInfo? best = null;
            foreach (var f in context.PendingFetches)
            {
                if (best == null || f.WaitingCustomers > best.WaitingCustomers)
                    best = f;
            }
            if (best != null)
                return CacheReadDecision.JoinFetch(best.Id);
        }

        // 3) Otherwise start a new fetch.
        return CacheReadDecision.StartFetch();
    }

    // -------- Cache fill: store with TTL, evict oldest on capacity pressure --------
    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        var value = context.Value;
        var ttl = value.SizeBytes < SmallEntrySizeThreshold ? SmallEntryTtl : LargeEntryTtl;

        // Compute used bytes excluding the entry we are about to (over)write.
        long used = 0;
        foreach (var e in context.Entries)
        {
            if (e.Key == value.Key) continue;
            used += e.SizeBytes;
        }

        // Fast path: there is room.
        if (used + value.SizeBytes <= context.CapacityBytes)
            return CacheWriteDecision.Retain(ttl);

        // Slow path: evict oldest entries (smallest SourceObservedAt = oldest data) until it fits.
        var victims = context.Entries
            .Where(e => e.Key != value.Key)
            .OrderBy(e => e.SourceObservedAt)
            .ToList();

        var evictions = new List<string>();
        long freed = 0;
        foreach (var v in victims)
        {
            if (used + value.SizeBytes - freed <= context.CapacityBytes) break;
            evictions.Add(v.Key);
            freed += v.SizeBytes;
        }

        // If still doesn't fit (entry larger than cache, or not enough victims), skip storing.
        if (used + value.SizeBytes - freed > context.CapacityBytes)
            return CacheWriteDecision.Skip();

        return CacheWriteDecision.Retain(ttl, evictions.ToArray());
    }

    // -------- Job assignment: reject doomed jobs, then highest-revenue -> fastest worker --------
    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0)
            return null;

        // 1) Reject jobs that provably cannot finish within the processing budget.
        JobInfo? doomed = null;
        foreach (var j in context.QueuedJobs)
        {
            if (j.Request.Kind != RequestKind.Generate) continue;
            double age = (context.Now - j.Request.CreatedAt).TotalSeconds;
            double worstCompletion = age + j.Request.EstimatedWorkSeconds * DoomWorkFactor;
            if (worstCompletion <= GenerateBudget.TotalSeconds - DoomGuardMarginSeconds) continue;
            if (doomed == null || j.QueuedAt < doomed.QueuedAt)
                doomed = j; // reject the oldest doomed job first
        }
        if (doomed != null)
            return JobAssignmentDecision.Reject(doomed.Id);

        // 2) Pick the highest-revenue job; oldest QueuedAt wins ties (FIFO within same priority).
        JobInfo? job = null;
        foreach (var j in context.QueuedJobs)
        {
            if (j.Request.Kind == RequestKind.Generate)
                _avgWorkSeconds += AvgWorkEmaAlpha * (j.Request.EstimatedWorkSeconds - _avgWorkSeconds);
            if (job == null) { job = j; continue; }
            if (j.Request.Revenue > job.Request.Revenue ||
                (j.Request.Revenue == job.Request.Revenue && j.QueuedAt < job.QueuedAt))
                job = j;
        }

        EndpointInfo? worker = null;
        double bestScore = double.MaxValue;
        foreach (var w in context.AvailableWorkers)
        {
            double score = -w.Speed; // lower is better -> faster worker wins
            if (score < bestScore)
            {
                bestScore = score;
                worker = w;
            }
        }

        if (job == null || worker == null) return null;
        return JobAssignmentDecision.Assign(job.Id, worker.Id);
    }

    // -------- Job recovery: requeue when worker appears stuck --------
    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var job = context.Job;
        var now = context.Now;

        // Give up if the assignment has been outstanding far longer than the work should take.
        if (job.AssignedAt.HasValue && (now - job.AssignedAt.Value) > JobGiveUpAfter)
            return JobRecoveryDecision.RequeueAfter(JobRequeueDelay);

        // Heartbeat-based detection: workers send heartbeats roughly every 500ms while doing work.
        if (job.LastHeartbeat.HasValue)
        {
            if ((now - job.LastHeartbeat.Value) > JobHeartbeatStaleLimit)
                return JobRecoveryDecision.RequeueAfter(JobRequeueDelay);
        }
        else if (job.AssignedAt.HasValue && (now - job.AssignedAt.Value) > JobNoHeartbeatLimit)
        {
            // No heartbeat ever received shortly after assignment - likely the worker is not making progress.
            return JobRecoveryDecision.RequeueAfter(JobRequeueDelay);
        }

        return JobRecoveryDecision.KeepWaiting();
    }

    // -------- Capacity plan: reactive + load-driven proactive --------
    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        var settings = context.Settings;


        int urgentJobs = 0;
        foreach (var j in context.QueuedJobs)
        {
            if (j.Request.Kind != RequestKind.Generate) continue;
            double age = (context.Now - j.Request.CreatedAt).TotalSeconds;
            if (age >= UrgencyHorizonSeconds) urgentJobs++;
        }
        _loadEma = _loadEma * (1.0 - LoadEmaAlpha) + urgentJobs * LoadEmaAlpha;

        int desiredApi = BaselineApiCount;

        // Payback sizing: the fleet must stay justified by the OBSERVED completion (drain) rate (workers doing real, payable work).
        double reviewSeconds = _lastCapacityReview.HasValue
            ? Math.Max(0.05, (context.Now - _lastCapacityReview.Value).TotalSeconds)
            : 1.0;
        double completionsPerSec = _completionsSinceLastReview / reviewSeconds;
        _completionsEma = _completionsEma * (1.0 - CompletionsEmaAlpha) + completionsPerSec * CompletionsEmaAlpha;
        _completionsSinceLastReview = 0;
        _lastCapacityReview = context.Now;

        // Asymmetric fleet hysteresis: grow only after sustained evidence, shrink immediately.
        int drainDerived = Math.Max(BaselineWorkerCount, (int)Math.Ceiling(_completionsEma * _avgWorkSeconds));
        if (drainDerived > _stabilizedFleet)
        {
            if (++_growStreak >= GrowConfirmReviews)
            {
                _stabilizedFleet = drainDerived;
                _growStreak = 0;
            }
        }
        else
        {
            _growStreak = 0;
            _stabilizedFleet = drainDerived; // shrink right away: idle capacity is pure cost
        }

        int drainFleet = _stabilizedFleet;
        int queueFleet = BaselineWorkerCount + (int)(Math.Max(0, _loadEma - ProbeHysteresis) / WorkersPerUrgentJob);
        int desiredWorker = _completionsEma > NoDrainProbeGuard
            ? Math.Max(drainFleet, Math.Min(drainFleet + 1, queueFleet))
            : BaselineWorkerCount;
        desiredWorker = Math.Min(desiredWorker, settings.MaximumWorkers);


        int apiCu = desiredApi * settings.Api.ComputeUnits;
        int remainingCu = settings.ComputeUnitLimit - apiCu;
        int maxWorkersByCu = remainingCu / settings.Worker.ComputeUnits;
        desiredWorker = Math.Min(desiredWorker, Math.Min(settings.MaximumWorkers, maxWorkersByCu));

        return new CapacityPlan(desiredApi, desiredWorker);
    }

    // -------- Observe: track job completion throughput for the payback cap --------
    public void Observe(SystemObservation observation)
    {
        if (observation.Kind == ObservationKind.JobCompleted)
            _completionsSinceLastReview++;
    }

    // ===== RPC policy =====

    private sealed class AdaptiveRequestPolicy : IRequestPolicy
    {
        public TimeSpan GetTimeout(RequestContext context, Random random) => context.Attempt switch
        {
            1 => TimeSpan.FromMilliseconds(250),
            2 => TimeSpan.FromMilliseconds(500),
            _ => TimeSpan.FromMilliseconds(900)
        };

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            switch (observation.Outcome)
            {
                case RequestOutcome.Success:
                case RequestOutcome.Rejected:
                case RequestOutcome.PolicyRejected:
                case RequestOutcome.Stale:
                    return RetryDecision.NoRetry();
                case RequestOutcome.Timeout:
                case RequestOutcome.Unavailable:
                    // Allow up to 2 retries (attempts 2 and 3) with a small jittered backoff.
                    if (observation.Attempt >= 3) return RetryDecision.NoRetry();
                    return RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(40 + random.Next(0, 60)));
                default:
                    return RetryDecision.NoRetry();
            }
        }
    }
}
