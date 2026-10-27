using Elsa.Common;
using Elsa.Common.Models;
using Elsa.Common.Multitenancy;
using Elsa.Common.Services;
using Elsa.Workflows.Management;
using Elsa.Workflows.Management.Entities;
using Elsa.Workflows.Management.Filters;
using Elsa.Workflows.Management.Models;
using Elsa.Workflows.Management.Stores;
using Elsa.Workflows.Models;
using Elsa.Workflows.Runtime.Options;
using Elsa.Workflows.Runtime.Requests;
using Elsa.Workflows.Runtime.Responses;
using Elsa.Workflows.Runtime.Services;
using Elsa.Workflows.State;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace Elsa.Workflows.Runtime.UnitTests.Recovery;

public class CancellingRecoveryScannerTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 27, 12, 0, 0, TimeSpan.Zero);

    private readonly IWorkflowCancellationDispatcher _dispatcher = Substitute.For<IWorkflowCancellationDispatcher>();
    private readonly IWorkflowInstanceStore _instanceStore = Substitute.For<IWorkflowInstanceStore>();
    private readonly ILogger<CancellingRecoveryScanner> _logger = Substitute.For<ILogger<CancellingRecoveryScanner>>();
    private readonly RuntimeOptions _runtimeOptions = new()
    {
        CancellingRecoveryThreshold = TimeSpan.FromMinutes(5),
        CancellingRecoveryBatchSize = 2
    };

    public CancellingRecoveryScannerTests()
    {
        _dispatcher.DispatchAsync(Arg.Any<DispatchCancelWorkflowRequest>(), Arg.Any<CancellationToken>())
            .Returns(new DispatchCancelWorkflowsResponse());
    }

    [Fact(DisplayName = "Scan claims a stale Cancelling row and re-dispatches without restore metadata")]
    public async Task ClaimsAndRedispatchesStaleCancellingRow()
    {
        var staleAt = Now.AddMinutes(-10);
        StubInstances([Summary("stale", staleAt)]);
        _instanceStore.TryTransitionSubStatusAsync(
                "stale",
                WorkflowSubStatus.Cancelling,
                staleAt,
                WorkflowSubStatus.Cancelling,
                Now,
                Arg.Any<CancellationToken>())
            .Returns(new ValueTask<bool>(true));
        var sut = BuildSut();

        var count = await sut.ScanAndRedispatchAsync(CancellationToken.None);

        Assert.Equal(1, count);
        await _instanceStore.Received(1).SummarizeManyAsync(
            Arg.Is<WorkflowInstanceFilter>(filter =>
                filter.WorkflowStatus == WorkflowStatus.Running
                && filter.WorkflowSubStatus == WorkflowSubStatus.Cancelling
                && filter.IsExecuting == false
                && filter.BeforeLastUpdated == Now - TimeSpan.FromMinutes(5)),
            Arg.Is<PageArgs>(page => page.Offset == 0 && page.Limit == 2),
            Arg.Any<CancellationToken>());
        await _dispatcher.Received(1).DispatchAsync(
            Arg.Is<DispatchCancelWorkflowRequest>(request =>
                request.WorkflowInstanceId == "stale"
                && request.PreviousSubStatus == null
                && request.CancellingAt == null),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "Scan skips re-dispatch when the Cancelling claim loses")]
    public async Task SkipsDispatchWhenClaimLoses()
    {
        var staleAt = Now.AddMinutes(-10);
        StubInstances([Summary("lost", staleAt), Summary("kept", staleAt)]);
        _instanceStore.TryTransitionSubStatusAsync(
                Arg.Any<string>(),
                WorkflowSubStatus.Cancelling,
                staleAt,
                WorkflowSubStatus.Cancelling,
                Now,
                Arg.Any<CancellationToken>())
            .Returns(call => new ValueTask<bool>(call.ArgAt<string>(0) == "kept"));
        var sut = BuildSut();

        var count = await sut.ScanAndRedispatchAsync(CancellationToken.None);

        Assert.Equal(1, count);
        await _dispatcher.DidNotReceive().DispatchAsync(
            Arg.Is<DispatchCancelWorkflowRequest>(request => request.WorkflowInstanceId == "lost"),
            Arg.Any<CancellationToken>());
        await _dispatcher.Received(1).DispatchAsync(
            Arg.Is<DispatchCancelWorkflowRequest>(request => request.WorkflowInstanceId == "kept"),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "A dispatch failure leaves the remaining stale rows to be re-dispatched")]
    public async Task ContinuesAfterDispatchFailure()
    {
        var staleAt = Now.AddMinutes(-10);
        StubInstances([Summary("fails", staleAt), Summary("next", staleAt)]);
        _instanceStore.TryTransitionSubStatusAsync(
                Arg.Any<string>(),
                Arg.Any<WorkflowSubStatus>(),
                Arg.Any<DateTimeOffset?>(),
                Arg.Any<WorkflowSubStatus>(),
                Arg.Any<DateTimeOffset>(),
                Arg.Any<CancellationToken>())
            .Returns(new ValueTask<bool>(true));
        _dispatcher.DispatchAsync(
                Arg.Is<DispatchCancelWorkflowRequest>(request => request.WorkflowInstanceId == "fails"),
                Arg.Any<CancellationToken>())
            .Returns<Task<DispatchCancelWorkflowsResponse>>(_ => throw new InvalidOperationException("enqueue failed"));
        var sut = BuildSut();

        var count = await sut.ScanAndRedispatchAsync(CancellationToken.None);

        Assert.Equal(1, count);
        await _dispatcher.Received(1).DispatchAsync(
            Arg.Is<DispatchCancelWorkflowRequest>(request => request.WorkflowInstanceId == "next"),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "A non-positive threshold does not query the store")]
    public async Task DisabledThresholdSkipsTheStore()
    {
        _runtimeOptions.CancellingRecoveryThreshold = TimeSpan.Zero;
        var sut = BuildSut();

        var count = await sut.ScanAndRedispatchAsync(CancellationToken.None);

        Assert.Equal(0, count);
        await _instanceStore.DidNotReceive().SummarizeManyAsync(
            Arg.Any<WorkflowInstanceFilter>(),
            Arg.Any<PageArgs>(),
            Arg.Any<CancellationToken>());
        await _dispatcher.DidNotReceive().DispatchAsync(Arg.Any<DispatchCancelWorkflowRequest>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "Tenant context is pushed before the claim and the re-dispatch")]
    public async Task PushesTenantContextBeforeClaim()
    {
        var staleAt = Now.AddMinutes(-10);
        StubInstances([Summary("stale", staleAt, tenantId: "tenant-1")]);
        var tenantService = Substitute.For<ITenantService>();
        tenantService.FindAsync("tenant-1", Arg.Any<CancellationToken>()).Returns(new Tenant { Id = "tenant-1", Name = "Tenant 1" });
        var tenantAccessor = new DefaultTenantAccessor();
        string? tenantAtClaim = null;
        string? tenantAtDispatch = null;
        _instanceStore.TryTransitionSubStatusAsync(
                Arg.Any<string>(),
                Arg.Any<WorkflowSubStatus>(),
                Arg.Any<DateTimeOffset?>(),
                Arg.Any<WorkflowSubStatus>(),
                Arg.Any<DateTimeOffset>(),
                Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                tenantAtClaim = tenantAccessor.Tenant?.Id;
                return new ValueTask<bool>(true);
            });
        _dispatcher.When(dispatcher => dispatcher.DispatchAsync(Arg.Any<DispatchCancelWorkflowRequest>(), Arg.Any<CancellationToken>()))
            .Do(_ => tenantAtDispatch = tenantAccessor.Tenant?.Id);

        var sut = BuildSut(tenantService, tenantAccessor);
        await sut.ScanAndRedispatchAsync(CancellationToken.None);

        Assert.Equal("tenant-1", tenantAtClaim);
        Assert.Equal("tenant-1", tenantAtDispatch);
        Assert.Null(tenantAccessor.Tenant);
    }

    [Fact(DisplayName = "Memory store re-dispatches only stale non-executing Cancelling rows")]
    public async Task MemoryStoreRedispatchesOnlyStaleCancellingRows()
    {
        var store = new MemoryWorkflowInstanceStore(new MemoryStore<WorkflowInstance>());
        var staleAt = Now.AddMinutes(-10);
        var recentAt = Now.AddMinutes(-1);
        await store.SaveAsync(Instance("stale-a", WorkflowStatus.Running, WorkflowSubStatus.Cancelling, staleAt));
        await store.SaveAsync(Instance("stale-b", WorkflowStatus.Running, WorkflowSubStatus.Cancelling, staleAt));
        await store.SaveAsync(Instance("recent", WorkflowStatus.Running, WorkflowSubStatus.Cancelling, recentAt));
        await store.SaveAsync(Instance("executing", WorkflowStatus.Running, WorkflowSubStatus.Cancelling, staleAt, isExecuting: true));
        await store.SaveAsync(Instance("suspended", WorkflowStatus.Running, WorkflowSubStatus.Suspended, staleAt));
        await store.SaveAsync(Instance("pending", WorkflowStatus.Running, WorkflowSubStatus.Pending, staleAt));
        await store.SaveAsync(Instance("interrupted", WorkflowStatus.Running, WorkflowSubStatus.Interrupted, staleAt));
        await store.SaveAsync(Instance("cancelled", WorkflowStatus.Finished, WorkflowSubStatus.Cancelled, staleAt));

        var options = new RuntimeOptions
        {
            CancellingRecoveryThreshold = TimeSpan.FromMinutes(5),
            CancellingRecoveryBatchSize = 1
        };
        var sut = new CancellingRecoveryScanner(
            _dispatcher,
            store,
            Microsoft.Extensions.Options.Options.Create(options),
            new FixedClock(Now),
            NullLogger<CancellingRecoveryScanner>.Instance);

        var count = await sut.ScanAndRedispatchAsync(CancellationToken.None);

        Assert.Equal(2, count);
        await _dispatcher.Received(1).DispatchAsync(
            Arg.Is<DispatchCancelWorkflowRequest>(request => request.WorkflowInstanceId == "stale-a" && request.PreviousSubStatus == null && request.CancellingAt == null),
            Arg.Any<CancellationToken>());
        await _dispatcher.Received(1).DispatchAsync(
            Arg.Is<DispatchCancelWorkflowRequest>(request => request.WorkflowInstanceId == "stale-b" && request.PreviousSubStatus == null && request.CancellingAt == null),
            Arg.Any<CancellationToken>());
        await _dispatcher.DidNotReceive().DispatchAsync(
            Arg.Is<DispatchCancelWorkflowRequest>(request => request.WorkflowInstanceId != "stale-a" && request.WorkflowInstanceId != "stale-b"),
            Arg.Any<CancellationToken>());

        var staleA = await store.FindAsync(new WorkflowInstanceFilter { Id = "stale-a" });
        var recent = await store.FindAsync(new WorkflowInstanceFilter { Id = "recent" });
        var executing = await store.FindAsync(new WorkflowInstanceFilter { Id = "executing" });
        var suspended = await store.FindAsync(new WorkflowInstanceFilter { Id = "suspended" });
        Assert.NotNull(staleA);
        Assert.Equal(WorkflowSubStatus.Cancelling, staleA.SubStatus);
        Assert.Equal(Now, staleA.UpdatedAt);
        Assert.Equal("bookmark", Assert.Single(staleA.WorkflowState.Bookmarks).Id);
        Assert.NotNull(recent);
        Assert.Equal(recentAt, recent.UpdatedAt);
        Assert.NotNull(executing);
        Assert.Equal(staleAt, executing.UpdatedAt);
        Assert.True(executing.IsExecuting);
        Assert.NotNull(suspended);
        Assert.Equal(WorkflowSubStatus.Suspended, suspended.SubStatus);
        Assert.Equal(staleAt, suspended.UpdatedAt);
    }

    private CancellingRecoveryScanner BuildSut(ITenantService? tenantService = null, ITenantAccessor? tenantAccessor = null) =>
        new(_dispatcher, _instanceStore, Microsoft.Extensions.Options.Options.Create(_runtimeOptions), new FixedClock(Now), _logger, tenantService, tenantAccessor);

    private void StubInstances(IReadOnlyList<WorkflowInstanceSummary> all)
    {
        _instanceStore.SummarizeManyAsync(Arg.Any<WorkflowInstanceFilter>(), Arg.Any<PageArgs>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var pageArgs = call.ArgAt<PageArgs>(1);
                var offset = pageArgs.Offset ?? 0;
                var limit = pageArgs.Limit ?? all.Count;
                var items = all.Skip(offset).Take(limit).ToList();
                return new ValueTask<Page<WorkflowInstanceSummary>>(Page.Of(items, all.Count));
            });
    }

    private static WorkflowInstanceSummary Summary(string id, DateTimeOffset updatedAt, string? tenantId = null) => new()
    {
        Id = id,
        TenantId = tenantId,
        DefinitionId = "def-" + id,
        DefinitionVersionId = "ver-" + id,
        Version = 1,
        Status = WorkflowStatus.Running,
        SubStatus = WorkflowSubStatus.Cancelling,
        CreatedAt = updatedAt,
        UpdatedAt = updatedAt
    };

    private static WorkflowInstance Instance(string id, WorkflowStatus status, WorkflowSubStatus subStatus, DateTimeOffset updatedAt, bool isExecuting = false) =>
        new()
        {
            Id = id,
            DefinitionId = "def-" + id,
            DefinitionVersionId = "ver-" + id,
            Version = 1,
            Status = status,
            SubStatus = subStatus,
            IsExecuting = isExecuting,
            CreatedAt = updatedAt,
            UpdatedAt = updatedAt,
            WorkflowState = new WorkflowState
            {
                Id = id,
                Status = status,
                SubStatus = subStatus,
                Bookmarks = [new Bookmark("bookmark", "Wait", "hash", null, "activity", "node", id, default)]
            }
        };

    private sealed class FixedClock(DateTimeOffset utcNow) : ISystemClock
    {
        public DateTimeOffset UtcNow => utcNow;
    }
}
